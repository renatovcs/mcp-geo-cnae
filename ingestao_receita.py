"""
Pipeline de Ingestão de Dados Reais da Receita Federal para Memgraph (GraphRAG).

Processa o dataset de 7,16 milhões de empresas ativas da Receita Federal
armazenado em /mnt/e/anotae/data/receita/processed/empresas_ativas.parquet,
agregando coocorrências econômicas e presenças municipais via DuckDB
e inserindo no Memgraph em lotes otimizados com UNWIND.
"""

import csv
import glob
import logging
import os
import time
import unicodedata
from typing import Any, Dict, List, Optional, Set, Tuple

import duckdb
from dotenv import load_dotenv
from geopy.distance import geodesic
from neo4j import Auth, Driver, GraphDatabase
from neo4j.exceptions import Neo4jError, ServiceUnavailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ingestao_receita")

load_dotenv()

PARQUET_PATH = os.getenv(
    "PARQUET_PATH", "/mnt/e/anotae/data/receita/processed/empresas_ativas.parquet"
)
RECEITA_DIR = os.getenv("RECEITA_DIR", "/mnt/e/anotae/data/receita")
MEMGRAPH_URI = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")
MEMGRAPH_USER = os.getenv("MEMGRAPH_USER", "")
MEMGRAPH_PASSWORD = os.getenv("MEMGRAPH_PASSWORD", "")


def normalizar_texto(texto: Optional[str]) -> str:
    """Normaliza strings removendo acentos e pontuações para matching tolerante."""
    if not texto:
        return ""
    n = unicodedata.normalize("NFKD", str(texto)).encode("ASCII", "ignore").decode("ASCII")
    return n.strip().upper().replace("-", " ")


def formatar_cnae(codigo_7digitos: str) -> str:
    """Converte '6201501' para a máscara padrão '6201-5/01'."""
    c = codigo_7digitos.strip()
    if len(c) == 7:
        return f"{c[:4]}-{c[4]}/{c[5:]}"
    return c


def obter_secao_cnae(codigo_7digitos: str) -> str:
    """Classifica a Seção oficial do CNAE (A até U) a partir dos dois primeiros dígitos (Divisão)."""
    if len(codigo_7digitos) < 2:
        return "OUTROS"
    try:
        div = int(codigo_7digitos[:2])
    except ValueError:
        return "OUTROS"

    if 1 <= div <= 3: return "A"
    if 5 <= div <= 9: return "B"
    if 10 <= div <= 33: return "C"
    if div == 35: return "D"
    if 36 <= div <= 39: return "E"
    if 41 <= div <= 43: return "F"
    if 45 <= div <= 47: return "G"
    if 49 <= div <= 53: return "H"
    if 55 <= div <= 56: return "I"
    if 58 <= div <= 63: return "J"
    if 64 <= div <= 66: return "K"
    if div == 68: return "L"
    if 69 <= div <= 75: return "M"
    if 77 <= div <= 82: return "N"
    if div == 84: return "O"
    if div == 85: return "P"
    if 86 <= div <= 88: return "Q"
    if 90 <= div <= 93: return "R"
    if 94 <= div <= 96: return "S"
    if div == 97: return "T"
    if div == 99: return "U"
    return "OUTROS"


class ReceitaGraphIngestor:
    """Orquestrador de carga e modelagem do grafo de dados da Receita Federal."""

    def __init__(self, uri: str, user: str = "", password: str = "") -> None:
        self.uri = uri
        self.user = user
        self.password = password
        self._driver: Optional[Driver] = None

    def connect(self) -> Driver:
        auth_token: Optional[Auth] = None
        if self.user or self.password:
            auth_token = (self.user, self.password)  # type: ignore[assignment]
        self._driver = GraphDatabase.driver(self.uri, auth=auth_token)
        self._driver.verify_connectivity()
        logger.info("Conectado ao Memgraph em %s", self.uri)
        return self._driver

    def close(self) -> None:
        if self._driver:
            self._driver.close()
            logger.info("Conexão com Memgraph encerrada.")

    def criar_constraints(self) -> None:
        """Cria constraints e índices para garantir performance e unicidade."""
        if not self._driver:
            raise RuntimeError("Driver não conectado.")

        queries = [
            "CREATE CONSTRAINT ON (c:CNAE) ASSERT c.codigo IS UNIQUE;",
            "CREATE CONSTRAINT ON (m:Municipio) ASSERT m.codigo_ibge IS UNIQUE;",
            "CREATE CONSTRAINT ON (e:Estado) ASSERT e.uf IS UNIQUE;",
            "CREATE INDEX ON :CNAE(codigo_raw);",
            "CREATE INDEX ON :Municipio(nome);",
        ]
        with self._driver.session() as s:
            for q in queries:
                try:
                    s.run(q)
                except Neo4jError as exc:
                    logger.info("Aviso em constraint/índice: %s", exc.message)

    def limpar_banco(self) -> None:
        """Limpa o grafo antes da carga completa."""
        if not self._driver:
            raise RuntimeError("Driver não conectado.")
        with self._driver.session() as s:
            s.run("MATCH (n) DETACH DELETE n;")
            logger.info("Grafo anterior limpo com sucesso.")

    def executar_batch(self, query: str, batch: List[Dict[str, Any]], desc: str) -> None:
        """Executa um lote de itens parametrizado com UNWIND."""
        if not self._driver:
            raise RuntimeError("Driver não conectado.")
        if not batch:
            return
        try:
            with self._driver.session() as s:
                s.run(query, batch=batch)
        except Exception as exc:
            logger.error("Erro ao executar lote '%s': %s", desc, exc)
            raise

    def processar_e_inserir_estados_e_municipios(
        self,
    ) -> Dict[Tuple[str, str], Dict[str, Any]]:
        """
        Carrega os municípios e estados das tabelas geográficas oficiais do IBGE,
        cria os nós (:Estado) e (:Municipio) e estabelece a relação [:PERTENCE_A].
        Retorna o mapa (municipio_norm, uf) -> {codigo_ibge, nome, lat, lon, uf}.
        """
        logger.info("Carregando tabelas geográficas do IBGE...")
        con = duckdb.connect()

        # 1. Estados
        estados = con.execute("SELECT uf, nome FROM 'estados.csv'").fetchall()
        batch_estados = [{"uf": r[0], "nome": r[1]} for r in estados]
        q_estados = """
        UNWIND $batch AS item
        MERGE (e:Estado {uf: item.uf})
        ON CREATE SET e.nome = item.nome
        ON MATCH SET e.nome = item.nome
        """
        self.executar_batch(q_estados, batch_estados, "Estados")
        logger.info("Inseridos %d estados.", len(batch_estados))

        # 2. Municípios presentes no parquet
        muns_parquet = con.execute(f"""
            SELECT DISTINCT municipio, uf 
            FROM '{PARQUET_PATH}'
            WHERE municipio IS NOT NULL AND uf IS NOT NULL
        """).fetchall()

        # Tabela completa de municípios IBGE
        muns_ibge = con.execute("""
            SELECT m.codigo_ibge, m.nome, e.uf, m.latitude, m.longitude
            FROM 'municipios.csv' m
            JOIN 'estados.csv' e ON m.codigo_uf = e.codigo_uf
        """).fetchall()

        ibge_lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for ibge, nome, uf, lat, lon in muns_ibge:
            key = (normalizar_texto(nome), uf.strip().upper())
            ibge_lookup[key] = {
                "codigo_ibge": str(ibge),
                "nome": nome,
                "uf": uf.strip().upper(),
                "lat": float(lat),
                "lon": float(lon),
            }

        municipios_para_inserir: Dict[str, Dict[str, Any]] = {}
        municipios_map: Dict[Tuple[str, str], Dict[str, Any]] = {}

        for mun, uf in muns_parquet:
            norm_key = (normalizar_texto(mun), uf.strip().upper())
            if norm_key in ibge_lookup:
                info = ibge_lookup[norm_key]
                municipios_para_inserir[info["codigo_ibge"]] = info
                municipios_map[norm_key] = info
            else:
                logger.warning("Município não localizado no IBGE: %s/%s", mun, uf)

        batch_muns = list(municipios_para_inserir.values())
        q_muns = """
        UNWIND $batch AS item
        MERGE (m:Municipio {codigo_ibge: item.codigo_ibge})
        ON CREATE SET m.nome = item.nome, m.lat = item.lat, m.lon = item.lon
        ON MATCH SET m.nome = item.nome, m.lat = item.lat, m.lon = item.lon
        WITH m, item
        MATCH (e:Estado {uf: item.uf})
        MERGE (m)-[:PERTENCE_A]->(e)
        """
        self.executar_batch(q_muns, batch_muns, "Municípios e PERTENCE_A")
        logger.info("Inseridos %d municípios com conexões a seus Estados.", len(batch_muns))

        return municipios_map

    def calcular_e_inserir_divisas(
        self, municipios_map: Dict[Tuple[str, str], Dict[str, Any]]
    ) -> None:
        """
        Calcula as distâncias geodésicas (em km) via Geopy entre municípios vizinhos
        e gera arestas (:Municipio)-[:FAZ_DIVISA_COM {distancia_km}]-(:Municipio).
        """
        logger.info("Calculando divisas e distâncias geodésicas entre municípios...")
        lista_muns = list({m["codigo_ibge"]: m for m in municipios_map.values()}.values())

        # Agrupa por UF para conectar vizinhos geográficos
        divisas_set: Set[Tuple[str, str, float]] = set()

        for i in range(len(lista_muns)):
            m1 = lista_muns[i]
            coord1 = (m1["lat"], m1["lon"])
            vizinhos_candidatos: List[Tuple[float, Dict[str, Any]]] = []

            for j in range(len(lista_muns)):
                if i == j:
                    continue
                m2 = lista_muns[j]
                # Apenas municípios do mesmo Estado ou Estados limítrofes
                coord2 = (m2["lat"], m2["lon"])
                dist_km = round(geodesic(coord1, coord2).km, 2)

                # Considera municípios limítrofes/adjacentes até 38km
                if dist_km <= 38.0:
                    vizinhos_candidatos.append((dist_km, m2))

            # Conecta os vizinhos mais próximos
            vizinhos_candidatos.sort(key=lambda x: x[0])
            for dist_km, m2 in vizinhos_candidatos[:5]:
                ibge_a, ibge_b = sorted([m1["codigo_ibge"], m2["codigo_ibge"]])
                divisas_set.add((ibge_a, ibge_b, dist_km))

        batch_divisas = [
            {"ibge_origem": d[0], "ibge_destino": d[1], "distancia_km": d[2]}
            for d in divisas_set
        ]

        q_divisas = """
        UNWIND $batch AS item
        MATCH (m1:Municipio {codigo_ibge: item.ibge_origem})
        MATCH (m2:Municipio {codigo_ibge: item.ibge_destino})
        MERGE (m1)-[r:FAZ_DIVISA_COM]->(m2)
        SET r.distancia_km = item.distancia_km
        """
        self.executar_batch(q_divisas, batch_divisas, "Arestas FAZ_DIVISA_COM")
        logger.info("Inseridas %d arestas de divisa territorial.", len(batch_divisas))

    def carregar_e_inserir_cnaes(self) -> Dict[str, Dict[str, str]]:
        """Carrega todos os CNAEs oficiais do arquivo da Receita e insere no Memgraph."""
        logger.info("Carregando catálogo oficial de CNAEs da Receita Federal...")
        cnae_files = glob.glob(os.path.join(RECEITA_DIR, "*CNAECSV*"))
        if not cnae_files:
            raise FileNotFoundError(f"Arquivo CNAECSV não encontrado em {RECEITA_DIR}")

        cnae_dict: Dict[str, Dict[str, str]] = {}
        with open(cnae_files[0], encoding="latin1") as f:
            reader = csv.reader(f, delimiter=";")
            for row in reader:
                if len(row) >= 2:
                    raw = row[0].strip()
                    desc = row[1].strip()
                    fmt = formatar_cnae(raw)
                    sec = obter_secao_cnae(raw)
                    cnae_dict[raw] = {
                        "codigo": fmt,
                        "codigo_raw": raw,
                        "descricao": desc,
                        "secao": sec,
                    }

        batch_cnaes = list(cnae_dict.values())
        q_cnaes = """
        UNWIND $batch AS item
        MERGE (c:CNAE {codigo: item.codigo})
        ON CREATE SET c.codigo_raw = item.codigo_raw, c.descricao = item.descricao, c.secao = item.secao
        ON MATCH SET c.codigo_raw = item.codigo_raw, c.descricao = item.descricao, c.secao = item.secao
        """
        self.executar_batch(q_cnaes, batch_cnaes, "Nós CNAE")
        logger.info("Inseridos %d nós de CNAE no grafo.", len(batch_cnaes))
        return cnae_dict

    def processar_e_inserir_coocorrencias_globais(self, min_freq: int = 20) -> None:
        """
        Extrai via DuckDB as coocorrências globais entre CNAE principal e secundário
        de empresas ativas em todo o Brasil e insere no Memgraph em lotes.
        """
        logger.info(
            "Extraindo coocorrências globais de empresas ativas (frequência >= %d)...",
            min_freq,
        )
        con = duckdb.connect()
        query = f"""
        WITH pairs AS (
            SELECT 
                cnae_fiscal_principal AS cnae_a,
                trim(unnest(string_split(cnae_fiscal_secundaria, ','))) AS cnae_b
            FROM '{PARQUET_PATH}'
            WHERE cnae_fiscal_secundaria IS NOT NULL 
              AND cnae_fiscal_secundaria != ''
              AND length(cnae_fiscal_principal) = 7
        ),
        canonical AS (
            SELECT 
                CASE WHEN cnae_a < cnae_b THEN cnae_a ELSE cnae_b END AS c1,
                CASE WHEN cnae_a < cnae_b THEN cnae_b ELSE cnae_a END AS c2,
                count(*) AS frequencia
            FROM pairs
            WHERE length(cnae_b) = 7 AND cnae_a != cnae_b
            GROUP BY c1, c2
            HAVING count(*) >= {min_freq}
        )
        SELECT c1, c2, frequencia FROM canonical ORDER BY frequencia DESC
        """
        df = con.execute(query).fetchall()
        logger.info("Total de coocorrências globais extraídas: %d", len(df))

        q_merge = """
        UNWIND $batch AS item
        MATCH (c1:CNAE {codigo: item.cnae_a})
        MATCH (c2:CNAE {codigo: item.cnae_b})
        MERGE (c1)-[r:COOCORRE_GERAL]->(c2)
        SET r.frequencia = item.frequencia
        """

        lote_tamanho = 5000
        batch: List[Dict[str, Any]] = []
        inseridos = 0

        for r in df:
            batch.append({
                "cnae_a": formatar_cnae(r[0]),
                "cnae_b": formatar_cnae(r[1]),
                "frequencia": int(r[2]),
            })
            if len(batch) >= lote_tamanho:
                self.executar_batch(q_merge, batch, "COOCORRE_GERAL")
                inseridos += len(batch)
                batch = []

        if batch:
            self.executar_batch(q_merge, batch, "COOCORRE_GERAL")
            inseridos += len(batch)

        logger.info("Concluída a inserção de %d arestas COOCORRE_GERAL.", inseridos)

    def processar_e_inserir_coocorrencias_locais(
        self,
        municipios_map: Dict[Tuple[str, str], Dict[str, Any]],
        min_freq: int = 15,
    ) -> None:
        """
        Extrai via DuckDB as coocorrências municipais entre CNAEs de empresas ativas
        e insere (:CNAE)-[:COOCORRE_EM {frequencia, codigo_ibge_municipio}]-(:CNAE).
        """
        logger.info(
            "Extraindo coocorrências municipais de empresas ativas (frequência >= %d)...",
            min_freq,
        )
        con = duckdb.connect()
        query = f"""
        WITH pairs AS (
            SELECT 
                municipio,
                uf,
                cnae_fiscal_principal AS cnae_a,
                trim(unnest(string_split(cnae_fiscal_secundaria, ','))) AS cnae_b
            FROM '{PARQUET_PATH}'
            WHERE cnae_fiscal_secundaria IS NOT NULL 
              AND cnae_fiscal_secundaria != ''
              AND length(cnae_fiscal_principal) = 7
        ),
        canonical AS (
            SELECT 
                municipio,
                uf,
                CASE WHEN cnae_a < cnae_b THEN cnae_a ELSE cnae_b END AS c1,
                CASE WHEN cnae_a < cnae_b THEN cnae_b ELSE cnae_a END AS c2,
                count(*) AS frequencia
            FROM pairs
            WHERE length(cnae_b) = 7 AND cnae_a != cnae_b
            GROUP BY municipio, uf, c1, c2
            HAVING count(*) >= {min_freq}
        )
        SELECT municipio, uf, c1, c2, frequencia FROM canonical ORDER BY frequencia DESC
        """
        df = con.execute(query).fetchall()
        logger.info("Total de coocorrências municipais extraídas: %d", len(df))

        q_merge = """
        UNWIND $batch AS item
        MATCH (c1:CNAE {codigo: item.cnae_a})
        MATCH (c2:CNAE {codigo: item.cnae_b})
        MERGE (c1)-[r:COOCORRE_EM {codigo_ibge_municipio: item.codigo_ibge}]->(c2)
        SET r.frequencia = item.frequencia
        """

        lote_tamanho = 5000
        batch: List[Dict[str, Any]] = []
        inseridos = 0

        for mun, uf, c1, c2, freq in df:
            norm_key = (normalizar_texto(mun), uf.strip().upper())
            if norm_key not in municipios_map:
                continue
            ibge = municipios_map[norm_key]["codigo_ibge"]
            batch.append({
                "cnae_a": formatar_cnae(c1),
                "cnae_b": formatar_cnae(c2),
                "codigo_ibge": ibge,
                "frequencia": int(freq),
            })
            if len(batch) >= lote_tamanho:
                self.executar_batch(q_merge, batch, "COOCORRE_EM")
                inseridos += len(batch)
                batch = []

        if batch:
            self.executar_batch(q_merge, batch, "COOCORRE_EM")
            inseridos += len(batch)

        logger.info("Concluída a inserção de %d arestas COOCORRE_EM.", inseridos)

    def processar_e_inserir_presencas(
        self, municipios_map: Dict[Tuple[str, str], Dict[str, Any]]
    ) -> None:
        """
        Extrai contagem de empresas ativas de cada CNAE por município
        e cria (:CNAE)-[:PRESENTE_EM {quantidade_empresas_ativas}]->(:Municipio).
        """
        logger.info("Extraindo presenças de empresas ativas por município...")
        con = duckdb.connect()
        query = f"""
        SELECT municipio, uf, cnae_fiscal_principal, count(*) AS total
        FROM '{PARQUET_PATH}'
        WHERE length(cnae_fiscal_principal) = 7
        GROUP BY municipio, uf, cnae_fiscal_principal
        HAVING count(*) >= 1
        """
        df = con.execute(query).fetchall()
        logger.info("Total de registros de presença extraídos: %d", len(df))

        q_merge = """
        UNWIND $batch AS item
        MATCH (c:CNAE {codigo: item.codigo_cnae})
        MATCH (m:Municipio {codigo_ibge: item.codigo_ibge})
        MERGE (c)-[r:PRESENTE_EM]->(m)
        SET r.quantidade_empresas_ativas = item.quantidade
        """

        lote_tamanho = 5000
        batch: List[Dict[str, Any]] = []
        inseridos = 0

        for mun, uf, cnae_raw, total in df:
            norm_key = (normalizar_texto(mun), uf.strip().upper())
            if norm_key not in municipios_map:
                continue
            ibge = municipios_map[norm_key]["codigo_ibge"]
            batch.append({
                "codigo_cnae": formatar_cnae(cnae_raw),
                "codigo_ibge": ibge,
                "quantidade": int(total),
            })
            if len(batch) >= lote_tamanho:
                self.executar_batch(q_merge, batch, "PRESENTE_EM")
                inseridos += len(batch)
                batch = []

        if batch:
            self.executar_batch(q_merge, batch, "PRESENTE_EM")
            inseridos += len(batch)

        logger.info("Concluída a inserção de %d arestas PRESENTE_EM.", inseridos)


def main() -> None:
    t0 = time.time()
    logger.info("=== INICIANDO PIPELINE DE CARGA REAL DA RECEITA FEDERAL ===")
    ingestor = ReceitaGraphIngestor(
        uri=MEMGRAPH_URI,
        user=MEMGRAPH_USER,
        password=MEMGRAPH_PASSWORD,
    )

    try:
        ingestor.connect()
        ingestor.criar_constraints()
        ingestor.limpar_banco()

        municipios_map = ingestor.processar_e_inserir_estados_e_municipios()
        ingestor.calcular_e_inserir_divisas(municipios_map)
        ingestor.carregar_e_inserir_cnaes()
        ingestor.processar_e_inserir_coocorrencias_globais(min_freq=20)
        ingestor.processar_e_inserir_coocorrencias_locais(municipios_map, min_freq=15)
        ingestor.processar_e_inserir_presencas(municipios_map)

        t_total = time.time() - t0
        logger.info(
            "=== PIPELINE CONCLUÍDO COM SUCESSO EM %.2f SEGUNDOS ===", t_total
        )
    finally:
        ingestor.close()


if __name__ == "__main__":
    main()
