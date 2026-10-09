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
    retornando os dados do município, seu Estado e todos os municípios limítrofes
    que fazem divisa territorial física com ele, incluindo a distância geodésica em km.

    Args:
        codigo_ibge: Código IBGE de 7 dígitos do município (ex: '3550308') ou nome do município.

    Returns:
        Dicionário estruturado com dados do município de origem, estado e lista de municípios vizinhos.
    """
    ibge_str = codigo_ibge.strip()
    query = """
    MATCH (m:Municipio)
    WHERE m.codigo_ibge = $ibge OR toLower(m.nome) = toLower($ibge)
    MATCH (m)-[:PERTENCE_A]->(e:Estado)
    OPTIONAL MATCH (m)-[r:FAZ_DIVISA_COM]-(v:Municipio)-[:PERTENCE_A]->(ve:Estado)
    WITH m, e, collect(DISTINCT CASE WHEN v IS NOT NULL THEN {
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
    com maior concentração de empresas ativas e os totais acumulados.

    Args:
        codigo_ibge: Código IBGE (ex: '3550308') ou nome do município (ex: 'São Paulo', 'Curitiba').
        top_cnaes: Quantidade de principais atividades econômicas a retornar (padrão: 10).

    Returns:
        Dicionário com dados do município, Estado, total de empresas ativas mapeadas e ranking de CNAEs.
    """
    ibge_str = codigo_ibge.strip()
    query = """
    MATCH (m:Municipio)
    WHERE m.codigo_ibge = $ibge OR toLower(m.nome) = toLower($ibge)
    MATCH (m)-[:PERTENCE_A]->(e:Estado)
    OPTIONAL MATCH (c:CNAE)-[r:PRESENTE_EM]->(m)
    WITH m, e, c, r
    ORDER BY r.quantidade_empresas_ativas DESC
    WITH m, e, 
         sum(r.quantidade_empresas_ativas) AS total_empresas_ativas,
         collect({
             codigo: c.codigo,
             descricao: c.descricao,
             secao: c.secao,
             empresas_ativas: r.quantidade_empresas_ativas
         })[0..$top_cnaes] AS top_atividades
    RETURN m.codigo_ibge AS codigo_ibge,
           m.nome AS nome,
           e.uf AS uf,
           e.nome AS estado_nome,
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
                "total_empresas_ativas_mapeadas": int(result["total_empresas_ativas"] or 0),
                "principais_cnaes": atividades,
            }
    except Neo4jError as exc:
        logger.error("Erro Cypher em buscar_perfil_economico_municipio: %s", exc.message)
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
