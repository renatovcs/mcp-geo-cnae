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
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

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
    WITH m.codigo_ibge AS ibge_real, m
    MATCH (c1:CNAE)
    WHERE c1.codigo = $fmt OR c1.codigo_raw = $raw
    OPTIONAL MATCH (c1)-[r:COOCORRE_EM {codigo_ibge_municipio: ibge_real}]-(c2:CNAE)
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
                if record["codigo"] is not None and record["frequencia"] is not None
            ]
            if registros:
                return registros

            # Fallback gracioso: combina coocorrência nacional com atividades ativas presentes no município
            fallback_query = """
            MATCH (m:Municipio)
            WHERE m.codigo_ibge = $ibge OR toLower(m.nome) = toLower($ibge)
            WITH m.codigo_ibge AS ibge_real, m
            MATCH (c1:CNAE)
            WHERE c1.codigo = $fmt OR c1.codigo_raw = $raw
            MATCH (c1)-[r:COOCORRE_GERAL]-(c2:CNAE)
            MATCH (c2)-[p:PRESENTE_EM]->(m)
            WHERE coalesce(p.estoque_ativas, p.quantidade_empresas_ativas, 0) > 0
            RETURN c2.codigo AS codigo,
                   c2.descricao AS descricao,
                   c2.secao AS secao,
                   r.frequencia AS frequencia,
                   ibge_real AS codigo_ibge
            ORDER BY r.frequencia DESC
            LIMIT $limite
            """
            fb_result = session.run(fallback_query, fmt=fmt, raw=raw, ibge=ibge_str, limite=limite)
            return [
                {
                    "codigo": record["codigo"],
                    "descricao": record["descricao"],
                    "secao": record["secao"],
                    "frequencia": int(record["frequencia"]),
                    "codigo_ibge": record["codigo_ibge"],
                }
                for record in fb_result
            ]
    except Neo4jError as exc:
        logger.error("Erro Cypher em buscar_coocorrencia_cnaes_municipio: %s", exc.message)
        raise RuntimeError(f"Falha na consulta ao banco de grafos: {exc.message}") from exc


@mcp.tool()
def buscar_malha_municipal_vizinha(codigo_ibge: str) -> Dict[str, Any]:
    """
    Consulta a topologia geográfica de um município brasileiro pelo seu código IBGE (ou nome),
    retornando os dados do município, Região Imediata e Intermediária (IBGE), Estado, Macrorregião
    e todos os municípios limítrofes que fazem divisa territorial física com ele.

    Args:
        codigo_ibge: Código IBGE de 7 dígitos do município (ex: '3550308') ou nome do município.

    Returns:
        Dicionário estruturado com dados do município, hierarquia regional completa do IBGE e lista de vizinhos.
    """
    ibge_str = codigo_ibge.strip()
    query = """
    MATCH (m:Municipio)
    WHERE m.codigo_ibge = $ibge OR toLower(m.nome) = toLower($ibge)
    OPTIONAL MATCH (m)-[:PERTENCE_A]->(ri:RegiaoImediata)
    OPTIONAL MATCH (ri)-[:PERTENCE_A]->(rint:RegiaoIntermediaria)
    OPTIONAL MATCH (rint)-[:PERTENCE_A]->(e:Estado)
    OPTIONAL MATCH (e)-[:PERTENCE_A]->(reg:Regiao)
    OPTIONAL MATCH (m)-[r:FAZ_DIVISA_COM]-(v:Municipio)
    OPTIONAL MATCH (v)-[:PERTENCE_A*1..3]->(ve:Estado)
    WITH m, e, reg, ri, rint, collect(DISTINCT CASE WHEN v IS NOT NULL THEN {
        codigo_ibge: v.codigo_ibge,
        nome: v.nome,
        uf: ve.uf,
        distancia_km: r.distancia_km
    } END) AS vizinhos_raw
    RETURN m.codigo_ibge AS codigo_ibge,
           m.nome AS nome,
           m.lat AS lat,
           m.lon AS lon,
           coalesce(m.regiao_imediata, ri.nome, '') AS regiao_imediata,
           coalesce(m.regiao_intermediaria, rint.nome, '') AS regiao_intermediaria,
           coalesce(m.microrregiao, '') AS microrregiao,
           coalesce(m.mesorregiao, '') AS mesorregiao,
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
                    "uf": str(item["uf"] or ""),
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
                "regiao_imediata_ibge": str(result["regiao_imediata"]),
                "regiao_intermediaria_ibge": str(result["regiao_intermediaria"]),
                "microrregiao": str(result["microrregiao"]),
                "mesorregiao": str(result["mesorregiao"]),
                "estado": {
                    "uf": str(result["estado_uf"] or ""),
                    "nome": str(result["estado_nome"] or ""),
                    "macrorregiao": str(result["regiao_nome"] or ""),
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
    com maior concentração de empresas ativas, Região Imediata/Intermediária do IBGE e indicadores de dinâmica/sobrevivência.

    Args:
        codigo_ibge: Código IBGE (ex: '3550308') ou nome do município (ex: 'São Paulo', 'Curitiba').
        top_cnaes: Quantidade de principais atividades econômicas a retornar (padrão: 10).

    Returns:
        Dicionário com dados do município, Região Imediata, Intermediária, Estado, total de empresas e ranking enriquecido.
    """
    ibge_str = codigo_ibge.strip()
    query = """
    MATCH (m:Municipio)
    WHERE m.codigo_ibge = $ibge OR toLower(m.nome) = toLower($ibge)
    OPTIONAL MATCH (m)-[:PERTENCE_A]->(ri:RegiaoImediata)
    OPTIONAL MATCH (ri)-[:PERTENCE_A]->(rint:RegiaoIntermediaria)
    OPTIONAL MATCH (rint)-[:PERTENCE_A]->(e:Estado)
    OPTIONAL MATCH (e)-[:PERTENCE_A]->(reg:Regiao)
    OPTIONAL MATCH (c:CNAE)-[r:PRESENTE_EM]->(m)
    WITH m, e, reg, ri, rint, c, r
    ORDER BY coalesce(r.estoque_ativas, r.quantidade_empresas_ativas, 0) DESC
    WITH m, e, reg, ri, rint,
         sum(coalesce(r.estoque_ativas, r.quantidade_empresas_ativas, 0)) AS total_empresas_ativas,
         sum(coalesce(r.estoque_mei, 0)) AS total_empresas_mei,
         sum(coalesce(r.estoque_simples, 0)) AS total_empresas_simples,
         collect({
             codigo: c.codigo,
             descricao: c.descricao,
             secao: c.secao,
             empresas_ativas: coalesce(r.estoque_ativas, r.quantidade_empresas_ativas, 0),
             estoque_mei: coalesce(r.estoque_mei, 0),
             estoque_simples: coalesce(r.estoque_simples, 0),
             taxa_mei: coalesce(r.taxa_mei, 0.0),
             abertas_ultimos_5_anos: coalesce(r.abertas_ultimos_5_anos, 0),
             encerradas_ultimos_5_anos: coalesce(r.encerradas_ultimos_5_anos, 0),
             taxa_sobrevivencia_5_anos: coalesce(r.taxa_sobrevivencia_5_anos, 0.0),
             saldo_recente: coalesce(r.saldo_recente, 0)
         })[0..$top_cnaes] AS top_atividades
    RETURN m.codigo_ibge AS codigo_ibge,
           m.nome AS nome,
           coalesce(m.regiao_imediata, ri.nome, '') AS regiao_imediata,
           coalesce(m.regiao_intermediaria, rint.nome, '') AS regiao_intermediaria,
           e.uf AS uf,
           e.nome AS estado_nome,
           reg.nome AS regiao_nome,
           total_empresas_ativas,
           total_empresas_mei,
           total_empresas_simples,
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
                    "estoque_mei": int(ativ.get("estoque_mei") or 0),
                    "estoque_simples": int(ativ.get("estoque_simples") or 0),
                    "taxa_mei": round(float(ativ.get("taxa_mei") or 0.0), 4),
                    "taxa_mei_percentual": f"{round(float(ativ.get('taxa_mei') or 0.0) * 100, 1)}%",
                    "abertas_ultimos_5_anos": int(ativ["abertas_ultimos_5_anos"]),
                    "encerradas_ultimos_5_anos": int(ativ["encerradas_ultimos_5_anos"]),
                    "taxa_sobrevivencia_5_anos": round(float(ativ["taxa_sobrevivencia_5_anos"]), 4),
                    "taxa_sobrevivencia_percentual": f"{round(float(ativ['taxa_sobrevivencia_5_anos']) * 100, 1)}%",
                    "saldo_recente": int(ativ["saldo_recente"]),
                }
                for ativ in result["top_atividades"]
                if ativ and ativ["codigo"] is not None
            ]

            total_ativas = int(result["total_empresas_ativas"] or 0)
            total_mei = int(result["total_empresas_mei"] or 0)
            total_simples = int(result["total_empresas_simples"] or 0)
            taxa_mei_geral = round(total_mei / total_ativas, 4) if total_ativas > 0 else 0.0

            return {
                "encontrado": True,
                "codigo_ibge": str(result["codigo_ibge"]),
                "nome": str(result["nome"]),
                "regiao_imediata_ibge": str(result["regiao_imediata"]),
                "regiao_intermediaria_ibge": str(result["regiao_intermediaria"]),
                "uf": str(result["uf"] or ""),
                "estado": str(result["estado_nome"] or ""),
                "regiao": str(result["regiao_nome"] or ""),
                "macrorregiao": str(result["regiao_nome"] or ""),
                "total_empresas_ativas_mapeadas": total_ativas,
                "total_empresas_mei": total_mei,
                "total_empresas_simples": total_simples,
                "taxa_mei_geral_municipio": taxa_mei_geral,
                "taxa_mei_geral_percentual": f"{round(taxa_mei_geral * 100, 1)}%",
                "principais_cnaes": atividades,
            }
    except Neo4jError as exc:
        logger.error("Erro Cypher em buscar_perfil_economico_municipio: %s", exc.message)
        raise RuntimeError(f"Falha na consulta ao banco de grafos: {exc.message}") from exc


@mcp.tool()
def buscar_perfil_economico_regiao_ibge(
    nome_regiao: str, top_cnaes: int = 10
) -> Dict[str, Any]:
    """
    Retorna o perfil econômico agregado de uma Região Geográfica do IBGE (Imediata ou Intermediária),
    somando as empresas ativas de todos os municípios que compõem aquele polo regional.

    Permite análises econômicas de polos regionais (ex: 'Ponta Grossa', 'Curitiba', 'Campinas', 'Ribeirão Preto').

    Args:
        nome_regiao: Nome da Região Imediata ou Intermediária (ex: 'Ponta Grossa', 'Campinas').
        top_cnaes: Quantidade de principais atividades CNAEs no ranking agregado (padrão: 10).

    Returns:
        Dicionário com a lista de municípios integrantes, total regional de empresas ativas e principais CNAEs.
    """
    regiao_str = nome_regiao.strip()
    query = """
    MATCH (ri)
    WHERE (ri:RegiaoImediata OR ri:RegiaoIntermediaria) AND toLower(ri.nome) = toLower($nome_regiao)
    MATCH (m:Municipio)-[:PERTENCE_A*1..2]->(ri)
    MATCH (m)-[:PERTENCE_A*1..3]->(e:Estado)
    OPTIONAL MATCH (c:CNAE)-[r:PRESENTE_EM]->(m)
    WITH ri, e, collect(DISTINCT m.nome) AS municipios, c, 
         sum(coalesce(r.estoque_ativas, r.quantidade_empresas_ativas, 0)) AS cnae_empresas_total,
         sum(coalesce(r.estoque_mei, 0)) AS cnae_mei_total,
         sum(coalesce(r.estoque_simples, 0)) AS cnae_simples_total
    ORDER BY cnae_empresas_total DESC
    WITH ri, e, municipios,
         sum(cnae_empresas_total) AS total_empresas_regiao,
         sum(cnae_mei_total) AS total_mei_regiao,
         sum(cnae_simples_total) AS total_simples_regiao,
         collect({
             codigo: c.codigo,
             descricao: c.descricao,
             secao: c.secao,
             empresas_ativas: cnae_empresas_total,
             estoque_mei: cnae_mei_total,
             estoque_simples: cnae_simples_total,
             taxa_mei: CASE WHEN cnae_empresas_total > 0 THEN (cnae_mei_total * 1.0 / cnae_empresas_total) ELSE 0.0 END
         })[0..$top_cnaes] AS top_atividades
    RETURN ri.nome AS nome_regiao,
           labels(ri)[0] AS tipo_regiao,
           e.uf AS uf,
           municipios,
           total_empresas_regiao,
           total_mei_regiao,
           total_simples_regiao,
           top_atividades
    LIMIT 1
    """
    try:
        with db_client.session() as session:
            result = session.run(query, nome_regiao=regiao_str, top_cnaes=top_cnaes).single()
            if not result or not result["nome_regiao"]:
                return {
                    "encontrado": False,
                    "termo_pesquisa": regiao_str,
                    "mensagem": f"Região geográfica '{regiao_str}' não localizada no IBGE.",
                }

            atividades = [
                {
                    "codigo": ativ["codigo"],
                    "descricao": ativ["descricao"],
                    "secao": ativ["secao"],
                    "empresas_ativas": int(ativ["empresas_ativas"]),
                    "estoque_mei": int(ativ.get("estoque_mei") or 0),
                    "estoque_simples": int(ativ.get("estoque_simples") or 0),
                    "taxa_mei": round(float(ativ.get("taxa_mei") or 0.0), 4),
                    "taxa_mei_percentual": f"{round(float(ativ.get('taxa_mei') or 0.0) * 100, 1)}%",
                }
                for ativ in result["top_atividades"]
                if ativ and ativ["codigo"] is not None
            ]

            total_regiao = int(result["total_empresas_regiao"] or 0)
            total_mei = int(result["total_mei_regiao"] or 0)
            total_simples = int(result["total_simples_regiao"] or 0)
            taxa_mei_regiao = round(total_mei / total_regiao, 4) if total_regiao > 0 else 0.0

            return {
                "encontrado": True,
                "regiao_nome": result["nome_regiao"],
                "tipo_regiao_ibge": "Região Imediata" if result["tipo_regiao"] == "RegiaoImediata" else "Região Intermediária",
                "uf": result["uf"],
                "total_municipios_integrantes": len(result["municipios"]),
                "municipios": sorted(result["municipios"]),
                "total_empresas_ativas_polo_regional": total_regiao,
                "total_empresas_mei_polo_regional": total_mei,
                "total_empresas_simples_polo_regional": total_simples,
                "taxa_mei_polo_regional": taxa_mei_regiao,
                "taxa_mei_polo_regional_percentual": f"{round(taxa_mei_regiao * 100, 1)}%",
                "principais_cnaes_polo_regional": atividades,
            }
    except Neo4jError as exc:
        logger.error("Erro Cypher em buscar_perfil_economico_regiao_ibge: %s", exc.message)
        raise RuntimeError(f"Falha na consulta ao banco de grafos: {exc.message}") from exc


@mcp.tool()
def buscar_dinamica_sobrevivencia_cnae(
    codigo_cnae: str, codigo_ibge: str
) -> Dict[str, Any]:
    """
    Consulta a dinâmica de mercado, ritmo de encerramentos, taxa de sobrevivência (safra de 5 anos)
    e composição de MEI vs Simples Nacional para uma atividade CNAE específica dentro de um município brasileiro.

    Baseado na metodologia formal Sebrae/IBGE (coorte de 5 anos), permitindo avaliar risco, viabilidade e maturidade de porte.

    Args:
        codigo_cnae: Código da atividade CNAE (ex: '6201-5/01' ou '6201501').
        codigo_ibge: Código IBGE (ex: '3550308') ou nome do município (ex: 'São Paulo').

    Returns:
        Dicionário com métricas de estoque ativo, divisão MEI/Simples/Geral, sobrevivência em 5 anos e diagnóstico de mercado.
    """
    fmt, raw = normalizar_cnae(codigo_cnae)
    ibge_str = codigo_ibge.strip()

    query = """
    MATCH (m:Municipio)
    WHERE m.codigo_ibge = $ibge OR toLower(m.nome) = toLower($ibge)
    MATCH (c:CNAE)
    WHERE c.codigo = $fmt OR c.codigo_raw = $raw
    OPTIONAL MATCH (m)-[:PERTENCE_A*1..3]->(e:Estado)
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
           r.estoque_mei AS estoque_mei,
           r.estoque_simples AS estoque_simples,
           r.taxa_mei AS taxa_mei,
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
            estoque_mei = int(record["estoque_mei"] or 0)
            estoque_simples = int(record["estoque_simples"] or 0)
            taxa_mei = float(record["taxa_mei"] or (round(estoque_mei / estoque, 4) if estoque > 0 else 0.0))
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

            # Perfil predominante de porte / formalização
            if taxa_mei >= 0.70:
                perfil_porte = "Predominância massiva de MEI (autônomos / microempreendedores individuais)"
            elif taxa_mei >= 0.40:
                perfil_porte = "Misto equilibrado (relevante presença de MEIs e PMEs estruturadas)"
            else:
                perfil_porte = "Predominância de empresas estruturadas (PMEs do Simples ou Médias/Grandes do Regime Geral)"

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
                    "estoque_mei": estoque_mei,
                    "estoque_simples": estoque_simples,
                    "estoque_outros_regimes": max(0, estoque - estoque_simples),
                    "taxa_mei": round(taxa_mei, 4),
                    "taxa_mei_percentual": f"{round(taxa_mei * 100, 1)}%",
                    "perfil_predominante_porte": perfil_porte,
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
    OPTIONAL MATCH (m:Municipio)-[:PERTENCE_A*1..3]->(e)
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


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request: Request) -> Response:
    """Verifica integridade do servidor MCP e conectividade com o banco Memgraph."""
    try:
        with db_client.session() as session:
            record = session.run("MATCH (m:Municipio) RETURN count(m) AS total;").single()
            total_muns = record["total"] if record else 0
        return JSONResponse(
            {
                "status": "healthy",
                "service": "mcp-geo-cnae",
                "memgraph": "connected",
                "total_municipios_indexados": total_muns,
            },
            status_code=200,
        )
    except Exception as exc:
        logger.error("Health check falhou: %s", exc)
        return JSONResponse(
            {
                "status": "unhealthy",
                "service": "mcp-geo-cnae",
                "memgraph": "disconnected",
                "detail": str(exc),
            },
            status_code=503,
        )


def run_server() -> None:
    try:
        raw_transport = os.getenv("MCP_TRANSPORT", "streamable-http").lower().replace("_", "-")
        transport = "streamable-http" if "http" in raw_transport else ("sse" if "sse" in raw_transport else "stdio")
        logger.info("Iniciando Servidor MCP Geo-CNAE (transporte: %s)...", transport)
        if transport in ("sse", "streamable-http"):
            host = os.getenv("MCP_HOST", os.getenv("HOST", "0.0.0.0"))
            port = int(os.getenv("PORT", os.getenv("MCP_PORT", "8000")))
            mcp.run(transport=transport, host=host, port=port)
        else:
            mcp.run(transport="stdio")
    except (KeyboardInterrupt, SystemExit):
        logger.info("Servidor MCP encerrado.")
    finally:
        db_client.close()


if __name__ == "__main__":
    run_server()
