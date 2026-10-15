"""
Módulo de Ingestão e Modelagem de Grafo para Memgraph (GraphRAG).

Modela o relacionamento de mercado entre CNAEs (empresas ativas)
e a malha territorial (municípios e estados) do Brasil.
"""

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from geopy.distance import geodesic
from neo4j import Auth, Driver, GraphDatabase
from neo4j.exceptions import Neo4jError, ServiceUnavailable

# Configuração de logging estruturado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ingestao_graph")

load_dotenv()


@dataclass(frozen=True)
class EstadoDTO:
    uf: str
    nome: str
    regiao: str


@dataclass(frozen=True)
class MunicipioDTO:
    codigo_ibge: str
    nome: str
    lat: float
    lon: float
    uf: str


@dataclass(frozen=True)
class CNAEDTO:
    codigo: str
    descricao: str
    secao: str


@dataclass(frozen=True)
class CoocorrenciaGlobalDTO:
    cnae_a: str
    cnae_b: str
    frequencia: int


@dataclass(frozen=True)
class CoocorrenciaLocalDTO:
    cnae_a: str
    cnae_b: str
    codigo_ibge: str
    frequencia: int


@dataclass(frozen=True)
class FilialExpansaoDTO:
    cnae_matriz: str
    cnae_filial: str
    frequencia: int


@dataclass(frozen=True)
class PresencaCNAEDTO:
    codigo_cnae: str
    codigo_ibge: str
    quantidade_empresas_ativas: int
    estoque_ativas: int
    abertas_ultimos_5_anos: int = 0
    encerradas_ultimos_5_anos: int = 0
    taxa_sobrevivencia_5_anos: float = 0.0
    saldo_recente: int = 0


class MemgraphIngestor:
    """Gerencia a conexão e ingestão de dados em lote no Memgraph."""

    def __init__(self, uri: str, auth: Optional[Tuple[str, str]] = None) -> None:
        self.uri: str = uri
        self.auth: Optional[Tuple[str, str]] = auth
        self._driver: Optional[Driver] = None

    def connect(self) -> Driver:
        """Estabelece a conexão com o banco de grafos Memgraph."""
        try:
            auth_token: Optional[Auth] = None
            if self.auth and (self.auth[0] or self.auth[1]):
                auth_token = self.auth  # type: ignore[assignment]

            self._driver = GraphDatabase.driver(self.uri, auth=auth_token)
            self._driver.verify_connectivity()
            logger.info("Conexão com Memgraph estabelecida com sucesso em %s", self.uri)
            return self._driver
        except ServiceUnavailable as exc:
            logger.error("Falha ao conectar ao Memgraph em %s: %s", self.uri, exc)
            raise
        except Exception as exc:
            logger.error("Erro inesperado durante a conexão com o Memgraph: %s", exc)
            raise

    def close(self) -> None:
        """Fecha a conexão do driver."""
        if self._driver:
            self._driver.close()
            logger.info("Driver Memgraph encerrado.")

    def criar_constraints(self) -> None:
        """Cria constraints de unicidade para os atributos chave do grafo."""
        if not self._driver:
            raise RuntimeError("Driver não inicializado. Chame connect() primeiro.")

        constraints_queries: List[str] = [
            "CREATE CONSTRAINT ON (c:CNAE) ASSERT c.codigo IS UNIQUE;",
            "CREATE CONSTRAINT ON (m:Municipio) ASSERT m.codigo_ibge IS UNIQUE;",
            "CREATE CONSTRAINT ON (e:Estado) ASSERT e.uf IS UNIQUE;",
            "CREATE CONSTRAINT ON (r:Regiao) ASSERT r.nome IS UNIQUE;",
            "CREATE INDEX ON :CNAE(codigo);",
            "CREATE INDEX ON :CNAE(codigo_raw);",
            "CREATE INDEX ON :Municipio(codigo_ibge);",
            "CREATE INDEX ON :Municipio(nome);",
            "CREATE INDEX ON :Estado(uf);",
            "CREATE INDEX ON :Regiao(nome);",
        ]

        with self._driver.session() as session:
            for query in constraints_queries:
                try:
                    session.run(query)
                    logger.info("Constraint/Índice processado: %s", query.strip())
                except Neo4jError as exc:
                    # Em caso de constraint ou índice já existente, loga em nível informativo
                    logger.info("Aviso de constraint/índice (pode já existir): %s", exc.message)
                except Exception as exc:
                    logger.warning("Falha ao registrar constraint/índice '%s': %s", query, exc)

    def inserir_estados(self, estados: List[EstadoDTO]) -> None:
        """Insere nós (:Regiao), (:Estado) e arestas (:Estado)-[:PERTENCE_A]->(:Regiao)."""
        if not self._driver:
            raise RuntimeError("Driver não inicializado.")

        query = """
        UNWIND $batch AS item
        MERGE (r:Regiao {nome: item.regiao})
        MERGE (e:Estado {uf: item.uf})
        ON CREATE SET e.nome = item.nome
        ON MATCH SET e.nome = item.nome
        MERGE (e)-[:PERTENCE_A]->(r)
        """
        payload: List[Dict[str, Any]] = [
            {"uf": e.uf, "nome": e.nome, "regiao": e.regiao} for e in estados
        ]
        self._executar_batch(query, payload, "Estados e Regiões")

    def inserir_municipios(self, municipios: List[MunicipioDTO]) -> None:
        """
        Insere nós (:Municipio {codigo_ibge, nome, lat, lon}) e cria arestas
        (:Municipio)-[:PERTENCE_A]->(:Estado).
        """
        if not self._driver:
            raise RuntimeError("Driver não inicializado.")

        query = """
        UNWIND $batch AS item
        MERGE (m:Municipio {codigo_ibge: item.codigo_ibge})
        ON CREATE SET m.nome = item.nome, m.lat = item.lat, m.lon = item.lon
        ON MATCH SET m.nome = item.nome, m.lat = item.lat, m.lon = item.lon
        WITH m, item
        MATCH (e:Estado {uf: item.uf})
        MERGE (m)-[:PERTENCE_A]->(e)
        """
        payload: List[Dict[str, Any]] = [
            {
                "codigo_ibge": m.codigo_ibge,
                "nome": m.nome,
                "lat": float(m.lat),
                "lon": float(m.lon),
                "uf": m.uf,
            }
            for m in municipios
        ]
        self._executar_batch(query, payload, "Municípios e Relações PERTENCE_A")

    def inserir_divisas(
        self,
        adjacencias: List[Tuple[str, str]],
        municipios_map: Dict[str, Tuple[float, float]],
    ) -> None:
        """
        Calcula a distância geodésica em km via Geopy entre municípios vizinhos
        e cria arestas (:Municipio)-[:FAZ_DIVISA_COM {distancia_km}]-(:Municipio).
        """
        if not self._driver:
            raise RuntimeError("Driver não inicializado.")

        divisas_payload: List[Dict[str, Any]] = []
        for ibge_a, ibge_b in adjacencias:
            if ibge_a not in municipios_map or ibge_b not in municipios_map:
                logger.warning(
                    "Ignorando divisa entre %s e %s: coordenadas não encontradas.",
                    ibge_a,
                    ibge_b,
                )
                continue

            coord_a = municipios_map[ibge_a]
            coord_b = municipios_map[ibge_b]
            dist_km: float = round(geodesic(coord_a, coord_b).km, 2)

            # Insere como aresta canônica ordenada lexicograficamente para unicidade
            ibge_1, ibge_2 = sorted([ibge_a, ibge_b])
            divisas_payload.append(
                {
                    "ibge_origem": ibge_1,
                    "ibge_destino": ibge_2,
                    "distancia_km": dist_km,
                }
            )

        query = """
        UNWIND $batch AS item
        MATCH (m1:Municipio {codigo_ibge: item.ibge_origem})
        MATCH (m2:Municipio {codigo_ibge: item.ibge_destino})
        MERGE (m1)-[r:FAZ_DIVISA_COM]->(m2)
        SET r.distancia_km = item.distancia_km
        """
        self._executar_batch(query, divisas_payload, "Arestas FAZ_DIVISA_COM")

    def inserir_cnaes(self, cnaes: List[CNAEDTO]) -> None:
        """Insere nós (:CNAE {codigo, descricao, secao}) em lote."""
        if not self._driver:
            raise RuntimeError("Driver não inicializado.")

        query = """
        UNWIND $batch AS item
        MERGE (c:CNAE {codigo: item.codigo})
        ON CREATE SET c.descricao = item.descricao, c.secao = item.secao
        ON MATCH SET c.descricao = item.descricao, c.secao = item.secao
        """
        payload: List[Dict[str, Any]] = [
            {"codigo": c.codigo, "descricao": c.descricao, "secao": c.secao}
            for c in cnaes
        ]
        self._executar_batch(query, payload, "Nós CNAE")

    def inserir_coocorrencias_globais(
        self, coocorrencias: List[CoocorrenciaGlobalDTO]
    ) -> None:
        """
        Insere arestas globais de coocorrência de mercado:
        (:CNAE)-[:COOCORRE_GERAL {frequencia}]-(:CNAE).
        """
        if not self._driver:
            raise RuntimeError("Driver não inicializado.")

        query = """
        UNWIND $batch AS item
        MATCH (c1:CNAE {codigo: item.cnae_a})
        MATCH (c2:CNAE {codigo: item.cnae_b})
        MERGE (c1)-[r:COOCORRE_GERAL]->(c2)
        SET r.frequencia = item.frequencia
        """
        payload: List[Dict[str, Any]] = []
        for cg in coocorrencias:
            cnae_1, cnae_2 = sorted([cg.cnae_a, cg.cnae_b])
            payload.append(
                {
                    "cnae_a": cnae_1,
                    "cnae_b": cnae_2,
                    "frequencia": int(cg.frequencia),
                }
            )
        self._executar_batch(query, payload, "Arestas COOCORRE_GERAL (Globais)")

    def inserir_coocorrencias_locais(
        self, coocorrencias: List[CoocorrenciaLocalDTO]
    ) -> None:
        """
        Insere arestas locais de coocorrência restritas a um município:
        (:CNAE)-[:COOCORRE_EM {frequencia, codigo_ibge_municipio}]-(:CNAE).
        """
        if not self._driver:
            raise RuntimeError("Driver não inicializado.")

        query = """
        UNWIND $batch AS item
        MATCH (c1:CNAE {codigo: item.cnae_a})
        MATCH (c2:CNAE {codigo: item.cnae_b})
        MERGE (c1)-[r:COOCORRE_EM {codigo_ibge_municipio: item.codigo_ibge}]->(c2)
        SET r.frequencia = item.frequencia
        """
        payload: List[Dict[str, Any]] = []
        for cl in coocorrencias:
            cnae_1, cnae_2 = sorted([cl.cnae_a, cl.cnae_b])
            payload.append(
                {
                    "cnae_a": cnae_1,
                    "cnae_b": cnae_2,
                    "codigo_ibge": cl.codigo_ibge,
                    "frequencia": int(cl.frequencia),
                }
            )
        self._executar_batch(query, payload, "Arestas COOCORRE_EM (Locais)")

    def inserir_expansao_filiais(
        self, expansoes: List[FilialExpansaoDTO]
    ) -> None:
        """
        Insere arestas de expansão corporativa Matriz -> Filiais:
        (:CNAE)-[:GERA_FILIAL {frequencia}]->(:CNAE).
        """
        if not self._driver:
            raise RuntimeError("Driver não inicializado.")

        query = """
        UNWIND $batch AS item
        MATCH (c1:CNAE {codigo: item.cnae_matriz})
        MATCH (c2:CNAE {codigo: item.cnae_filial})
        MERGE (c1)-[r:GERA_FILIAL]->(c2)
        SET r.frequencia = item.frequencia
        """
        payload: List[Dict[str, Any]] = [
            {
                "cnae_matriz": exp.cnae_matriz,
                "cnae_filial": exp.cnae_filial,
                "frequencia": int(exp.frequencia),
            }
            for exp in expansoes
        ]
        self._executar_batch(query, payload, "Arestas GERA_FILIAL (Matriz -> Filial)")

    def inserir_presenca_cnaes(self, presencas: List[PresencaCNAEDTO]) -> None:
        """
        Insere arestas de presença econômica e dinâmica de sobrevivência:
        (:CNAE)-[:PRESENTE_EM {estoque_ativas, abertas_ultimos_5_anos, ...}]->(:Municipio).
        """
        if not self._driver:
            raise RuntimeError("Driver não inicializado.")

        query = """
        UNWIND $batch AS item
        MATCH (c:CNAE {codigo: item.codigo_cnae})
        MATCH (m:Municipio {codigo_ibge: item.codigo_ibge})
        MERGE (c)-[r:PRESENTE_EM]->(m)
        SET r.quantidade_empresas_ativas = item.quantidade_empresas_ativas,
            r.estoque_ativas = item.estoque_ativas,
            r.abertas_ultimos_5_anos = item.abertas_ultimos_5_anos,
            r.encerradas_ultimos_5_anos = item.encerradas_ultimos_5_anos,
            r.taxa_sobrevivencia_5_anos = item.taxa_sobrevivencia_5_anos,
            r.saldo_recente = item.saldo_recente
        """
        payload: List[Dict[str, Any]] = [
            {
                "codigo_cnae": p.codigo_cnae,
                "codigo_ibge": p.codigo_ibge,
                "quantidade_empresas_ativas": int(p.quantidade_empresas_ativas),
                "estoque_ativas": int(p.estoque_ativas),
                "abertas_ultimos_5_anos": int(p.abertas_ultimos_5_anos),
                "encerradas_ultimos_5_anos": int(p.encerradas_ultimos_5_anos),
                "taxa_sobrevivencia_5_anos": float(p.taxa_sobrevivencia_5_anos),
                "saldo_recente": int(p.saldo_recente),
            }
            for p in presencas
        ]
        self._executar_batch(query, payload, "Arestas PRESENTE_EM Enriquecidas")

    def _executar_batch(
        self, query: str, batch: List[Dict[str, Any]], descricao: str
    ) -> None:
        """Executa uma query parametrizada com UNWIND com tratamento de erros."""
        if not self._driver:
            raise RuntimeError("Driver não inicializado.")
        if not batch:
            logger.info("Batch vazio para '%s'. Nenhuma operação realizada.", descricao)
            return

        try:
            with self._driver.session() as session:
                result = session.run(query, batch=batch)
                summary = result.consume()
                logger.info(
                    "Sucesso no lote '%s': %d itens processados. Nós criados: %d, Arestas: %d",
                    descricao,
                    len(batch),
                    summary.counters.nodes_created,
                    summary.counters.relationships_created,
                )
        except Neo4jError as exc:
            logger.error("Erro Cypher ao executar lote '%s': %s (código: %s)", descricao, exc.message, exc.code)
            raise
        except Exception as exc:
            logger.error("Erro inesperado no lote '%s': %s", descricao, exc)
            raise


def obter_dataset_sintetico() -> Tuple[
    List[EstadoDTO],
    List[MunicipioDTO],
    List[Tuple[str, str]],
    List[CNAEDTO],
    List[CoocorrenciaGlobalDTO],
    List[CoocorrenciaLocalDTO],
    List[PresencaCNAEDTO],
]:
    """
    Retorna o dataset sintético estruturado para teste imediato:
    - 2 Estados (SP, RJ)
    - 4 Municípios (São Paulo, Guarulhos, Rio de Janeiro, Duque de Caxias)
    - Lista explícita de adjacências de fronteira
    - 4 CNAEs de TI e Comércio
    - Coocorrências simuladas de empresas ativas
    """
    estados: List[EstadoDTO] = [
        EstadoDTO(uf="SP", nome="São Paulo", regiao="Sudeste"),
        EstadoDTO(uf="RJ", nome="Rio de Janeiro", regiao="Sudeste"),
    ]

    municipios: List[MunicipioDTO] = [
        # São Paulo e Guarulhos compartilham divisa na RMSP
        MunicipioDTO(codigo_ibge="3550308", nome="São Paulo", lat=-23.5505, lon=-46.6333, uf="SP"),
        MunicipioDTO(codigo_ibge="3518800", nome="Guarulhos", lat=-23.4542, lon=-46.5333, uf="SP"),
        # Rio de Janeiro e Duque de Caxias compartilham divisa na RMRJ
        MunicipioDTO(codigo_ibge="3304557", nome="Rio de Janeiro", lat=-22.9068, lon=-43.1729, uf="RJ"),
        MunicipioDTO(codigo_ibge="3301702", nome="Duque de Caxias", lat=-22.7858, lon=-43.3117, uf="RJ"),
    ]

    # Lista explícita de adjacência (fronteira física real)
    divisas_adjacencia: List[Tuple[str, str]] = [
        ("3550308", "3518800"),  # São Paulo <-> Guarulhos
        ("3304557", "3301702"),  # Rio de Janeiro <-> Duque de Caxias
    ]

    cnaes: List[CNAEDTO] = [
        CNAEDTO(
            codigo="6201-5/01",
            descricao="Desenvolvimento de programas de computador sob encomenda",
            secao="J",
        ),
        CNAEDTO(
            codigo="6202-3/00",
            descricao="Desenvolvimento e licenciamento de programas de computador customizáveis",
            secao="J",
        ),
        CNAEDTO(
            codigo="6204-0/00",
            descricao="Consultoria em tecnologia da informação",
            secao="J",
        ),
        CNAEDTO(
            codigo="4751-2/01",
            descricao="Comércio varejista especializado de equipamentos e suprimentos de informática",
            secao="G",
        ),
    ]

    # Expansão corporativa de Matriz para Filiais
    expansoes_filiais: List[FilialExpansaoDTO] = [
        FilialExpansaoDTO(cnae_matriz="6201-5/01", cnae_filial="6204-0/00", frequencia=3150),
        FilialExpansaoDTO(cnae_matriz="6201-5/01", cnae_filial="6202-3/00", frequencia=2480),
        FilialExpansaoDTO(cnae_matriz="6202-3/00", cnae_filial="6204-0/00", frequencia=1820),
    ]

    # Coocorrências em empresas ativas em âmbito nacional
    coocorrencias_globais: List[CoocorrenciaGlobalDTO] = [
        CoocorrenciaGlobalDTO(cnae_a="6201-5/01", cnae_b="6202-3/00", frequencia=1850),
        CoocorrenciaGlobalDTO(cnae_a="6201-5/01", cnae_b="6204-0/00", frequencia=1340),
        CoocorrenciaGlobalDTO(cnae_a="6201-5/01", cnae_b="4751-2/01", frequencia=420),
        CoocorrenciaGlobalDTO(cnae_a="6202-3/00", cnae_b="6204-0/00", frequencia=890),
        CoocorrenciaGlobalDTO(cnae_a="6202-3/00", cnae_b="4751-2/01", frequencia=260),
    ]

    # Coocorrências em empresas ativas por município
    coocorrencias_locais: List[CoocorrenciaLocalDTO] = [
        # São Paulo (3550308)
        CoocorrenciaLocalDTO(cnae_a="6201-5/01", cnae_b="6202-3/00", codigo_ibge="3550308", frequencia=1100),
        CoocorrenciaLocalDTO(cnae_a="6201-5/01", cnae_b="6204-0/00", codigo_ibge="3550308", frequencia=810),
        CoocorrenciaLocalDTO(cnae_a="6201-5/01", cnae_b="4751-2/01", codigo_ibge="3550308", frequencia=230),
        # Guarulhos (3518800)
        CoocorrenciaLocalDTO(cnae_a="6201-5/01", cnae_b="6202-3/00", codigo_ibge="3518800", frequencia=180),
        CoocorrenciaLocalDTO(cnae_a="6201-5/01", cnae_b="4751-2/01", codigo_ibge="3518800", frequencia=95),
        # Rio de Janeiro (3304557)
        CoocorrenciaLocalDTO(cnae_a="6201-5/01", cnae_b="6202-3/00", codigo_ibge="3304557", frequencia=490),
        CoocorrenciaLocalDTO(cnae_a="6201-5/01", cnae_b="6204-0/00", codigo_ibge="3304557", frequencia=380),
        # Duque de Caxias (3301702)
        CoocorrenciaLocalDTO(cnae_a="6201-5/01", cnae_b="6202-3/00", codigo_ibge="3301702", frequencia=80),
        CoocorrenciaLocalDTO(cnae_a="6201-5/01", cnae_b="4751-2/01", codigo_ibge="3301702", frequencia=45),
    ]

    # Presença de empresas ativas e dinâmicas de sobrevivência por CNAE em cada município
    presencas: List[PresencaCNAEDTO] = [
        PresencaCNAEDTO(codigo_cnae="6201-5/01", codigo_ibge="3550308", quantidade_empresas_ativas=14200, estoque_ativas=14200, abertas_ultimos_5_anos=4970, encerradas_ultimos_5_anos=1392, taxa_sobrevivencia_5_anos=0.720, saldo_recente=745),
        PresencaCNAEDTO(codigo_cnae="6202-3/00", codigo_ibge="3550308", quantidade_empresas_ativas=9800, estoque_ativas=9800, abertas_ultimos_5_anos=3430, encerradas_ultimos_5_anos=960, taxa_sobrevivencia_5_anos=0.720, saldo_recente=514),
        PresencaCNAEDTO(codigo_cnae="6204-0/00", codigo_ibge="3550308", quantidade_empresas_ativas=7300, estoque_ativas=7300, abertas_ultimos_5_anos=2555, encerradas_ultimos_5_anos=715, taxa_sobrevivencia_5_anos=0.720, saldo_recente=383),
        PresencaCNAEDTO(codigo_cnae="4751-2/01", codigo_ibge="3550308", quantidade_empresas_ativas=3100, estoque_ativas=3100, abertas_ultimos_5_anos=1085, encerradas_ultimos_5_anos=304, taxa_sobrevivencia_5_anos=0.720, saldo_recente=162),
        PresencaCNAEDTO(codigo_cnae="6201-5/01", codigo_ibge="3518800", quantidade_empresas_ativas=1850, estoque_ativas=1850, abertas_ultimos_5_anos=647, encerradas_ultimos_5_anos=181, taxa_sobrevivencia_5_anos=0.720, saldo_recente=97),
        PresencaCNAEDTO(codigo_cnae="6202-3/00", codigo_ibge="3518800", quantidade_empresas_ativas=1200, estoque_ativas=1200, abertas_ultimos_5_anos=420, encerradas_ultimos_5_anos=117, taxa_sobrevivencia_5_anos=0.721, saldo_recente=63),
        PresencaCNAEDTO(codigo_cnae="4751-2/01", codigo_ibge="3518800", quantidade_empresas_ativas=890, estoque_ativas=890, abertas_ultimos_5_anos=311, encerradas_ultimos_5_anos=87, taxa_sobrevivencia_5_anos=0.720, saldo_recente=46),
        PresencaCNAEDTO(codigo_cnae="6201-5/01", codigo_ibge="3304557", quantidade_empresas_ativas=7900, estoque_ativas=7900, abertas_ultimos_5_anos=2765, encerradas_ultimos_5_anos=774, taxa_sobrevivencia_5_anos=0.720, saldo_recente=414),
        PresencaCNAEDTO(codigo_cnae="6202-3/00", codigo_ibge="3304557", quantidade_empresas_ativas=5400, estoque_ativas=5400, abertas_ultimos_5_anos=1890, encerradas_ultimos_5_anos=529, taxa_sobrevivencia_5_anos=0.720, saldo_recente=283),
        PresencaCNAEDTO(codigo_cnae="6204-0/00", codigo_ibge="3304557", quantidade_empresas_ativas=4100, estoque_ativas=4100, abertas_ultimos_5_anos=1435, encerradas_ultimos_5_anos=401, taxa_sobrevivencia_5_anos=0.721, saldo_recente=215),
        PresencaCNAEDTO(codigo_cnae="6201-5/01", codigo_ibge="3301702", quantidade_empresas_ativas=950, estoque_ativas=950, abertas_ultimos_5_anos=332, encerradas_ultimos_5_anos=93, taxa_sobrevivencia_5_anos=0.720, saldo_recente=50),
        PresencaCNAEDTO(codigo_cnae="4751-2/01", codigo_ibge="3301702", quantidade_empresas_ativas=620, estoque_ativas=620, abertas_ultimos_5_anos=217, encerradas_ultimos_5_anos=61, taxa_sobrevivencia_5_anos=0.719, saldo_recente=32),
    ]

    return (
        estados,
        municipios,
        divisas_adjacencia,
        cnaes,
        expansoes_filiais,
        coocorrencias_globais,
        coocorrencias_locais,
        presencas,
    )


def main() -> None:
    """Função principal que orquestra a ingestão completa do dataset sintético."""
    uri: str = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")
    user: str = os.getenv("MEMGRAPH_USER", "")
    password: str = os.getenv("MEMGRAPH_PASSWORD", "")

    auth: Optional[Tuple[str, str]] = (user, password) if user or password else None
    ingestor = MemgraphIngestor(uri=uri, auth=auth)

    try:
        ingestor.connect()
        logger.info("Iniciando processo de criação de constraints...")
        ingestor.criar_constraints()

        logger.info("Carregando dataset sintético...")
        (
            estados,
            municipios,
            divisas_adjacencia,
            cnaes,
            expansoes_filiais,
            coocorrencias_globais,
            coocorrencias_locais,
            presencas,
        ) = obter_dataset_sintetico()

        # Mapa de coordenadas para cálculo geodésico
        municipios_coords: Dict[str, Tuple[float, float]] = {
            m.codigo_ibge: (m.lat, m.lon) for m in municipios
        }

        logger.info("Inserindo Estados e Regiões...")
        ingestor.inserir_estados(estados)

        logger.info("Inserindo Municípios e relacionamentos territoriais...")
        ingestor.inserir_municipios(municipios)

        logger.info("Calculando distâncias geodésicas e inserindo fronteiras...")
        ingestor.inserir_divisas(divisas_adjacencia, municipios_coords)

        logger.info("Inserindo CNAEs...")
        ingestor.inserir_cnaes(cnaes)

        logger.info("Inserindo Relações de Expansão Matriz -> Filiais...")
        ingestor.inserir_expansao_filiais(expansoes_filiais)

        logger.info("Inserindo Coocorrências Globais...")
        ingestor.inserir_coocorrencias_globais(coocorrencias_globais)

        logger.info("Inserindo Coocorrências Locais...")
        ingestor.inserir_coocorrencias_locais(coocorrencias_locais)

        logger.info("Inserindo Presenças e Dinâmicas de Sobrevivência...")
        ingestor.inserir_presenca_cnaes(presencas)

        logger.info("Pipeline de Ingestão e Modelagem de Grafo concluído com sucesso!")
    except Exception as exc:
        logger.exception("Falha durante o pipeline de ingestão: %s", exc)
        raise
    finally:
        ingestor.close()


if __name__ == "__main__":
    main()
