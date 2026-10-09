"""
Servidor MCP (Model Context Protocol) para GraphRAG de CNAEs e Topologia Municipal.

Integrado ao Memgraph com dados reais de 7,16 milhões de empresas ativas da Receita Federal.
Permite consultas analíticas de coocorrência de mercado, perfil econômico local e topologia de fronteiras.
"""

import logging
import os
import re
import sys
import unicodedata
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional, Tuple

from dotenv import load_dotenv
from neo4j import Auth, Driver, GraphDatabase, Session
from neo4j.exceptions import Neo4jError, ServiceUnavailable

try:
    from mcp.server.fastmcp import FastMCP
except (ImportError, ModuleNotFoundError):
    from mcp.server.mcpserver import MCPServer as FastMCP  # type: ignore[no-redef]

load_dotenv()

# Logging direcionado ao stderr para não corromper o canal stdio do protocolo MCP
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("mcp_geo_cnae")


def normalizar_cnae(cnae: str) -> Tuple[str, str]:
    """Retorna a versão formatada '6201-5/01' e a versão limpa '6201501'."""
    raw = re.sub(r"\D", "", cnae.strip())
    if len(raw) == 7:
        fmt = f"{raw[:4]}-{raw[4]}/{raw[5:]}"
        return fmt, raw
    return cnae.strip(), raw


def normalizar_municipio(texto: str) -> str:
    """Remove acentos e espaços extras para matching tolerante de cidades."""
    if not texto:
        return ""
    n = unicodedata.normalize("NFKD", texto).encode("ASCII", "ignore").decode("ASCII")
    return n.strip().upper().replace("-", " ")


class MemgraphClient:
    """Gerenciador da conexão e execução de consultas no Memgraph."""

    def __init__(self, uri: str, user: str = "", password: str = "") -> None:
        self.uri: str = uri
        self.user: str = user
        self.password: str = password
        self._driver: Optional[Driver] = None

    def get_driver(self) -> Driver:
        if self._driver is None:
            try:
                auth_token: Optional[Auth] = None
                if self.user or self.password:
                    auth_token = (self.user, self.password)  # type: ignore[assignment]
                self._driver = GraphDatabase.driver(self.uri, auth=auth_token)
                self._driver.verify_connectivity()
                logger.info("Conexão do servidor MCP com Memgraph ativa em %s", self.uri)
            except ServiceUnavailable as exc:
                logger.error("Erro de conexão com Memgraph (%s): %s", self.uri, exc)
                raise ConnectionError(
                    f"Não foi possível conectar ao Memgraph em {self.uri}. Verifique se o container está rodando."
                ) from exc
            except Exception as exc:
                logger.error("Erro inesperado ao criar driver Memgraph: %s", exc)
                raise
        return self._driver

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        driver = self.get_driver()
        session = driver.session()
        try:
            yield session
        finally:
            session.close()

    def close(self) -> None:
        if self._driver is not None:
            self._driver.close()
            self._driver = None
            logger.info("Driver Memgraph encerrado.")


MEMGRAPH_URI = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")
MEMGRAPH_USER = os.getenv("MEMGRAPH_USER", "")
MEMGRAPH_PASSWORD = os.getenv("MEMGRAPH_PASSWORD", "")

db_client = MemgraphClient(
    uri=MEMGRAPH_URI,
    user=MEMGRAPH_USER,
    password=MEMGRAPH_PASSWORD,
)

mcp = FastMCP("geo-cnae-graphrag")


@mcp.tool()
def buscar_coocorrencia_cnaes_global(
    codigo_cnae: str, limite: int = 20
) -> List[Dict[str, Any]]:
    """
    Busca os CNAEs que mais coocorrem com um determinado código CNAE em âmbito nacional,
    considerando exclusivamente empresas ATIVAS cadastradas na Receita Federal.

    Args:
        codigo_cnae: Código da atividade CNAE no formato com máscara (ex: '6201-5/01')
                     ou numérico (ex: '6201501').
        limite: Quantidade máxima de resultados (padrão: 20).

    Returns:
        Lista de dicionários contendo os CNAEs coocorrentes ordenados por frequência decrescente.
    """
    fmt, raw = normalizar_cnae(codigo_cnae)
    query = """
    MATCH (c1:CNAE)
    WHERE c1.codigo = $fmt OR c1.codigo_raw = $raw
    MATCH (c1)-[r:COOCORRE_GERAL]-(c2:CNAE)
    RETURN c2.codigo AS codigo,
           c2.descricao AS descricao,
           c2.secao AS secao,
           r.frequencia AS frequencia
    ORDER BY r.frequencia DESC
    LIMIT $limite
    """
    try:
        with db_client.session() as session:
            result = session.run(query, fmt=fmt, raw=raw, limite=limite)
            registros = [
                {
                    "codigo": record["codigo"],
                    "descricao": record["descricao"],
                    "secao": record["secao"],
                    "frequencia": int(record["frequencia"]),
                }
                for record in result
            ]
            return registros
    except Neo4jError as exc:
        logger.error("Erro Cypher em buscar_coocorrencia_cnaes_global: %s", exc.message)
        raise RuntimeError(f"Falha na consulta ao banco de grafos: {exc.message}") from exc


@mcp.tool()
def buscar_coocorrencia_cnaes_municipio(
    codigo_cnae: str, codigo_ibge: str, limite: int = 20
) -> List[Dict[str, Any]]:
    """
    Busca os CNAEs que mais coocorrem com um determinado código CNAE especificamente
    dentro do território de um município brasileiro (código IBGE de 7 dígitos ou nome),
    considerando exclusivamente empresas ativas na Receita Federal.

    Args:
        codigo_cnae: Código da atividade CNAE (ex: '6201-5/01' ou '6201501').
        codigo_ibge: Código IBGE de 7 dígitos do município (ex: '3550308' para São Paulo)
                     ou nome do município.
        limite: Quantidade máxima de resultados (padrão: 20).

    Returns:
        Lista de dicionários contendo os CNAEs coocorrentes no município, ordenados por frequência.
    """
    fmt, raw = normalizar_cnae(codigo_cnae)
    ibge_str = codigo_ibge.strip()

    query = """
    MATCH (m:Municipio)
    WHERE m.codigo_ibge = $ibge OR toLower(m.nome) = toLower($ibge)
    WITH m.codigo_ibge AS ibge_real
    MATCH (c1:CNAE)
    WHERE c1.codigo = $fmt OR c1.codigo_raw = $raw
    MATCH (c1)-[r:COOCORRE_EM {codigo_ibge_municipio: ibge_real}]-(c2:CNAE)
    RETURN c2.codigo AS codigo,
           c2.descricao AS descricao,
           c2.secao AS secao,
           r.frequencia AS frequencia,
           ibge_real AS codigo_ibge
    ORDER BY r.frequencia DESC
    LIMIT $limite
    """
    try:
        with db_client.session() as session:
            result = session.run(query, fmt=fmt, raw=raw, ibge=ibge_str, limite=limite)
            registros = [
                {
                    "codigo": record["codigo"],
                    "descricao": record["descricao"],
                    "secao": record["secao"],
                    "frequencia": int(record["frequencia"]),
                    "codigo_ibge": record["codigo_ibge"],
                }
                for record in result
            ]
            return registros
    except Neo4jError as exc:
        logger.error("Erro Cypher em buscar_coocorrencia_cnaes_municipio: %s", exc.message)
        raise RuntimeError(f"Falha na consulta ao banco de grafos: {exc.message}") from exc


@mcp.tool()
def buscar_malha_municipal_vizinha(codigo_ibge: str) -> Dict[str, Any]:
    """
    Consulta a topologia geográfica de um município brasileiro pelo seu código IBGE (ou nome),
    retornando os dados do município, seu Estado, Região do Brasil e todos os municípios limítrofes
    que fazem divisa territorial física com ele, incluindo a distância geodésica em km.

    Args:
        codigo_ibge: Código IBGE de 7 dígitos do município (ex: '3550308') ou nome do município.

    Returns:
        Dicionário estruturado com dados do município de origem, estado, região e lista de vizinhos.
    """
    ibge_str = codigo_ibge.strip()
    query = """
    MATCH (m:Municipio)
    WHERE m.codigo_ibge = $ibge OR toLower(m.nome) = toLower($ibge)
    MATCH (m)-[:PERTENCE_A]->(e:Estado)
    OPTIONAL MATCH (e)-[:PERTENCE_A]->(reg:Regiao)
    OPTIONAL MATCH (m)-[r:FAZ_DIVISA_COM]-(v:Municipio)-[:PERTENCE_A]->(ve:Estado)
    WITH m, e, reg, collect(DISTINCT CASE WHEN v IS NOT NULL THEN {
        codigo_ibge: v.codigo_ibge,
        nome: v.nome,
        uf: ve.uf,
        distancia_km: r.distancia_km
    } END) AS vizinhos_raw
    RETURN m.codigo_ibge AS codigo_ibge,
           m.nome AS nome,
           m.lat AS lat,
           m.lon AS lon,
           e.uf AS estado_uf,
           e.nome AS estado_nome,
           reg.nome AS regiao_nome,
           [viz IN vizinhos_raw WHERE viz IS NOT NULL] AS vizinhos
    """
    try:
        with db_client.session() as session:
            result = session.run(query, ibge=ibge_str).single()
            if not result:
                return {
                    "encontrado": False,
                    "codigo_ibge": ibge_str,
                    "mensagem": f"Município '{ibge_str}' não encontrado na base de dados.",
                    "total_vizinhos": 0,
                    "municipios_vizinhos": [],
                }

            vizinhos_lista: List[Dict[str, Any]] = [
                {
                    "codigo_ibge": str(item["codigo_ibge"]),
                    "nome": str(item["nome"]),
                    "uf": str(item["uf"]),
                    "distancia_km": float(item["distancia_km"]),
                }
                for item in result["vizinhos"]
                if item is not None and "distancia_km" in item and item["distancia_km"] is not None
            ]

            vizinhos_lista.sort(key=lambda x: x["distancia_km"])

            return {
                "encontrado": True,
                "codigo_ibge": str(result["codigo_ibge"]),
                "nome": str(result["nome"]),
                "lat": float(result["lat"]),
                "lon": float(result["lon"]),
                "estado": {
                    "uf": str(result["estado_uf"]),
                    "nome": str(result["estado_nome"]),
                    "regiao": str(result["regiao_nome"] or ""),
                },
                "total_vizinhos": len(vizinhos_lista),
                "municipios_vizinhos": vizinhos_lista,
            }
    except Neo4jError as exc:
        logger.error("Erro Cypher em buscar_malha_municipal_vizinha: %s", exc.message)
        raise RuntimeError(f"Falha na consulta ao banco de grafos: {exc.message}") from exc


@mcp.tool()
def buscar_perfil_economico_municipio(
    codigo_ibge: str, top_cnaes: int = 10
) -> Dict[str, Any]:
    """
    Retorna o perfil econômico consolidado de um município, listando as atividades (CNAEs)
    com maior concentração de empresas ativas, região geográfica e indicadores de dinâmica/sobrevivência.

    Args:
        codigo_ibge: Código IBGE (ex: '3550308') ou nome do município (ex: 'São Paulo', 'Curitiba').
        top_cnaes: Quantidade de principais atividades econômicas a retornar (padrão: 10).

    Returns:
        Dicionário com dados do município, Estado, Região, total de empresas ativas e ranking de CNAEs enriquecido.
    """
    ibge_str = codigo_ibge.strip()
    query = """
    MATCH (m:Municipio)
    WHERE m.codigo_ibge = $ibge OR toLower(m.nome) = toLower($ibge)
    MATCH (m)-[:PERTENCE_A]->(e:Estado)
    OPTIONAL MATCH (e)-[:PERTENCE_A]->(reg:Regiao)
    OPTIONAL MATCH (c:CNAE)-[r:PRESENTE_EM]->(m)
    WITH m, e, reg, c, r
    ORDER BY coalesce(r.estoque_ativas, r.quantidade_empresas_ativas, 0) DESC
    WITH m, e, reg,
         sum(coalesce(r.estoque_ativas, r.quantidade_empresas_ativas, 0)) AS total_empresas_ativas,
         collect({
             codigo: c.codigo,
             descricao: c.descricao,
             secao: c.secao,
             empresas_ativas: coalesce(r.estoque_ativas, r.quantidade_empresas_ativas, 0),
             abertas_ultimos_5_anos: coalesce(r.abertas_ultimos_5_anos, 0),
             encerradas_ultimos_5_anos: coalesce(r.encerradas_ultimos_5_anos, 0),
             taxa_sobrevivencia_5_anos: coalesce(r.taxa_sobrevivencia_5_anos, 0.0),
             saldo_recente: coalesce(r.saldo_recente, 0)
         })[0..$top_cnaes] AS top_atividades
    RETURN m.codigo_ibge AS codigo_ibge,
           m.nome AS nome,
           e.uf AS uf,
           e.nome AS estado_nome,
           reg.nome AS regiao_nome,
           total_empresas_ativas,
           top_atividades
    """
    try:
        with db_client.session() as session:
            result = session.run(query, ibge=ibge_str, top_cnaes=top_cnaes).single()
            if not result or result["codigo_ibge"] is None:
                return {
                    "encontrado": False,
                    "termo_pesquisa": ibge_str,
                    "mensagem": f"Município '{ibge_str}' não encontrado.",
                }

            atividades = [
                {
                    "codigo": ativ["codigo"],
                    "descricao": ativ["descricao"],
                    "secao": ativ["secao"],
                    "empresas_ativas": int(ativ["empresas_ativas"]),
                    "abertas_ultimos_5_anos": int(ativ["abertas_ultimos_5_anos"]),
                    "encerradas_ultimos_5_anos": int(ativ["encerradas_ultimos_5_anos"]),
                    "taxa_sobrevivencia_5_anos": round(float(ativ["taxa_sobrevivencia_5_anos"]), 4),
                    "taxa_sobrevivencia_percentual": f"{round(float(ativ['taxa_sobrevivencia_5_anos']) * 100, 1)}%",
                    "saldo_recente": int(ativ["saldo_recente"]),
                }
                for ativ in result["top_atividades"]
                if ativ and ativ["codigo"] is not None
            ]

            return {
                "encontrado": True,
                "codigo_ibge": str(result["codigo_ibge"]),
                "nome": str(result["nome"]),
                "uf": str(result["uf"]),
                "estado": str(result["estado_nome"]),
                "regiao": str(result["regiao_nome"] or ""),
                "total_empresas_ativas_mapeadas": int(result["total_empresas_ativas"] or 0),
                "principais_cnaes": atividades,
            }
    except Neo4jError as exc:
        logger.error("Erro Cypher em buscar_perfil_economico_municipio: %s", exc.message)
        raise RuntimeError(f"Falha na consulta ao banco de grafos: {exc.message}") from exc


@mcp.tool()
def buscar_dinamica_sobrevivencia_cnae(
    codigo_cnae: str, codigo_ibge: str
) -> Dict[str, Any]:
    """
    Consulta a dinâmica de mercado, ritmo de encerramentos e taxa de sobrevivência (safra de 5 anos)
    para uma atividade CNAE específica dentro de um município brasileiro.

    Baseado na metodologia formal Sebrae/IBGE (coorte de 5 anos), permitindo avaliar risco e viabilidade.

    Args:
        codigo_cnae: Código da atividade CNAE (ex: '6201-5/01' ou '6201501').
        codigo_ibge: Código IBGE (ex: '3550308') ou nome do município (ex: 'São Paulo').

    Returns:
        Dicionário com métricas de estoque ativo, sobrevivência em 5 anos, saldo recente e diagnóstico de mercado.
    """
    fmt, raw = normalizar_cnae(codigo_cnae)
    ibge_str = codigo_ibge.strip()

    query = """
    MATCH (m:Municipio)
    WHERE m.codigo_ibge = $ibge OR toLower(m.nome) = toLower($ibge)
    MATCH (c:CNAE)
    WHERE c.codigo = $fmt OR c.codigo_raw = $raw
    MATCH (m)-[:PERTENCE_A]->(e:Estado)
    OPTIONAL MATCH (e)-[:PERTENCE_A]->(reg:Regiao)
    OPTIONAL MATCH (c)-[r:PRESENTE_EM]->(m)
    RETURN m.codigo_ibge AS codigo_ibge,
           m.nome AS municipio_nome,
           e.uf AS uf,
           e.nome AS estado_nome,
           reg.nome AS regiao_nome,
           c.codigo AS cnae_codigo,
           c.descricao AS cnae_descricao,
           c.secao AS cnae_secao,
           r.estoque_ativas AS estoque_ativas,
           r.quantidade_empresas_ativas AS quantidade_legado,
           r.abertas_ultimos_5_anos AS abertas_5a,
           r.encerradas_ultimos_5_anos AS encerradas_5a,
           r.taxa_sobrevivencia_5_anos AS taxa_sobrevivencia,
           r.saldo_recente AS saldo_recente
    """
    try:
        with db_client.session() as session:
            record = session.run(query, fmt=fmt, raw=raw, ibge=ibge_str).single()
            if not record or not record["municipio_nome"]:
                return {
                    "encontrado": False,
                    "mensagem": f"Município '{ibge_str}' ou CNAE '{codigo_cnae}' não encontrado.",
                }

            estoque = int(record["estoque_ativas"] or record["quantidade_legado"] or 0)
            abertas_5a = int(record["abertas_5a"] or 0)
            encerradas_5a = int(record["encerradas_5a"] or 0)
            taxa_sob = float(record["taxa_sobrevivencia"] or 0.0)
            saldo_rec = int(record["saldo_recente"] or 0)

            # Diagnóstico mercadológico interpretável
            if taxa_sob >= 0.75:
                diagnostico = "Alta resiliência setorial (sobrevivência acima de 75% na safra de 5 anos)"
            elif taxa_sob >= 0.55:
                diagnostico = "Resiliência moderada / padrão de mercado (sobrevivência entre 55% e 75%)"
            else:
                diagnostico = "Alto risco de encerramento / mercado com churn elevado"

            return {
                "encontrado": True,
                "cnae": {
                    "codigo": record["cnae_codigo"],
                    "descricao": record["cnae_descricao"],
                    "secao": record["cnae_secao"],
                },
                "municipio": {
                    "codigo_ibge": record["codigo_ibge"],
                    "nome": record["municipio_nome"],
                    "uf": record["uf"],
                    "estado": record["estado_nome"],
                    "regiao": record["regiao_nome"] or "",
                },
                "indicadores_mercado": {
                    "estoque_empresas_ativas": estoque,
                    "abertas_ultimos_5_anos": abertas_5a,
                    "encerradas_ultimos_5_anos": encerradas_5a,
                    "taxa_sobrevivencia_5_anos": round(taxa_sob, 4),
                    "taxa_sobrevivencia_percentual": f"{round(taxa_sob * 100, 1)}%",
                    "taxa_mortalidade_precoce_percentual": f"{round((1.0 - taxa_sob) * 100, 1)}%",
                    "saldo_recente": saldo_rec,
                    "diagnostico_setorial": diagnostico,
                },
            }
    except Neo4jError as exc:
        logger.error("Erro Cypher em buscar_dinamica_sobrevivencia_cnae: %s", exc.message)
        raise RuntimeError(f"Falha na consulta ao banco de grafos: {exc.message}") from exc


@mcp.tool()
def buscar_expansao_filiais_cnae(
    codigo_cnae: str, limite: int = 10
) -> List[Dict[str, Any]]:
    """
    Retorna os ramos de atividades econômicas secundárias mais comuns abertos como FILIAIS
    a partir da atividade principal da MATRIZ (relação agregada Matriz -> Filial).

    Permite identificar padrões corporativos de expansão, verticalização e distribuição.

    Args:
        codigo_cnae: Código da atividade CNAE da matriz (ex: '6201-5/01' ou '6201501').
        limite: Quantidade máxima de CNAEs de filiais a retornar (padrão: 10).

    Returns:
        Lista com as atividades de filiais mais geradas ordenadas por frequência.
    """
    fmt, raw = normalizar_cnae(codigo_cnae)
    query = """
    MATCH (c1:CNAE)
    WHERE c1.codigo = $fmt OR c1.codigo_raw = $raw
    MATCH (c1)-[r:GERA_FILIAL]->(c2:CNAE)
    RETURN c2.codigo AS codigo,
           c2.descricao AS descricao,
           c2.secao AS secao,
           r.frequencia AS frequencia
    ORDER BY r.frequencia DESC
    LIMIT $limite
    """
    try:
        with db_client.session() as session:
            result = session.run(query, fmt=fmt, raw=raw, limite=limite)
            registros = [
                {
                    "codigo": record["codigo"],
                    "descricao": record["descricao"],
                    "secao": record["secao"],
                    "frequencia_filiais": int(record["frequencia"]),
                }
                for record in result
            ]
            return registros
    except Neo4jError as exc:
        logger.error("Erro Cypher em buscar_expansao_filiais_cnae: %s", exc.message)
        raise RuntimeError(f"Falha na consulta ao banco de grafos: {exc.message}") from exc


@mcp.tool()
def buscar_visao_macro_regiao(nome_regiao: str) -> Dict[str, Any]:
    """
    Retorna a composição geográfica e densidade econômica de uma das 5 macrorregiões do Brasil
    (Norte, Nordeste, Centro-Oeste, Sudeste ou Sul), listando seus Estados e municípios mapeados.

    Args:
        nome_regiao: Nome da macrorregião (ex: 'Sul', 'Sudeste', 'Nordeste', 'Norte', 'Centro-Oeste').

    Returns:
        Dicionário com a lista de estados, municípios e contagem total de presenças empresariais.
    """
    regiao_str = nome_regiao.strip()
    query = """
    MATCH (r:Regiao)
    WHERE toLower(r.nome) = toLower($regiao)
    MATCH (e:Estado)-[:PERTENCE_A]->(r)
    OPTIONAL MATCH (m:Municipio)-[:PERTENCE_A]->(e)
    WITH r, e, collect({
        codigo_ibge: m.codigo_ibge,
        nome: m.nome
    }) AS municipios_estado
    RETURN r.nome AS regiao,
           collect({
               uf: e.uf,
               nome: e.nome,
               total_municipios_mapeados: size([m IN municipios_estado WHERE m.codigo_ibge IS NOT NULL]),
               municipios: [m IN municipios_estado WHERE m.codigo_ibge IS NOT NULL]
           }) AS estados
    """
    try:
        with db_client.session() as session:
            record = session.run(query, regiao=regiao_str).single()
            if not record or not record["regiao"]:
                return {
                    "encontrado": False,
                    "termo_pesquisa": regiao_str,
                    "mensagem": f"Macrorregião '{regiao_str}' não encontrada. Opções válidas: Norte, Nordeste, Centro-Oeste, Sudeste, Sul.",
                }

            estados_lista = record["estados"]
            total_muns = sum(item["total_municipios_mapeados"] for item in estados_lista)

            return {
                "encontrado": True,
                "regiao": record["regiao"],
                "total_estados": len(estados_lista),
                "total_municipios_mapeados": total_muns,
                "estados": estados_lista,
            }
    except Neo4jError as exc:
        logger.error("Erro Cypher em buscar_visao_macro_regiao: %s", exc.message)
        raise RuntimeError(f"Falha na consulta ao banco de grafos: {exc.message}") from exc


def run_server() -> None:
    try:
        logger.info("Iniciando Servidor MCP Geo-CNAE...")
        mcp.run()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Servidor MCP encerrado.")
    finally:
        db_client.close()


if __name__ == "__main__":
    run_server()
