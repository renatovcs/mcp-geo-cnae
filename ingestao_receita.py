"""
Pipeline de Ingestão de Dados Oficiais da Receita Federal para Memgraph (GraphRAG Nacional).

Processa os arquivos brutos da Receita Federal diretamente do diretório 'dados_receita':
- Estabelecimentos (*.ESTABELE ou Estabelecimentos*.zip)
- Catálogo de CNAEs (*.CNAECSV ou Cnaes.zip)
- Mapeamento territorial completo dos 5.570 municípios do Brasil via SIAFI/TOM -> IBGE
- Dinâmicas de sobrevivência empresarial (Sebrae/IBGE - 5 anos)
- Coocorrências econômicas e expansão corporativa Matriz -> Filiais ([:GERA_FILIAL])

Executa em streaming com DuckDB em memória (:memory:) sem gerar arquivos intermediários em disco.
"""

import csv
import glob
import io
import logging
import os
import time
import unicodedata
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import duckdb
from dotenv import load_dotenv
from geopy.distance import geodesic
from neo4j import Auth, Driver, GraphDatabase
from neo4j.exceptions import Neo4jError, ServiceUnavailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ingestao_receita_brasil")

load_dotenv()

# Diretórios e Configurações de Conexão
MEMGRAPH_URI = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")
MEMGRAPH_USER = os.getenv("MEMGRAPH_USER", "")
MEMGRAPH_PASSWORD = os.getenv("MEMGRAPH_PASSWORD", "")

POSSIVEIS_DIRS_RECEITA = [
    os.getenv("DADOS_RECEITA_DIR"),
    "/dados_receita",
    "/home/ubuntu/dados_receita",
    "./dados_receita",
    "../dados_receita",
]


def normalizar_texto(texto: Optional[str]) -> str:
    """Normaliza strings removendo acentuação para matching tolerante."""
    if not texto:
        return ""
    n = unicodedata.normalize("NFKD", str(texto)).encode("ASCII", "ignore").decode("ASCII")
    return n.strip().upper().replace("-", " ")


def formatar_cnae(codigo_7digitos: str) -> str:
    """Converte '6201501' para a máscara padrão oficial '6201-5/01'."""
    c = codigo_7digitos.strip()
    if len(c) == 7:
        return f"{c[:4]}-{c[4]}/{c[5:]}"
    return c


def obter_secao_cnae(codigo_7digitos: str) -> str:
    """Classifica a Seção oficial do CNAE (A até U) a partir dos dois primeiros dígitos."""
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
    """Orquestrador da modelagem e ingestão do grafo nacional da Receita Federal."""

    def __init__(self, uri: str, user: str = "", password: str = "") -> None:
        self.uri = uri
        self.user = user
        self.password = password
        self._driver: Optional[Driver] = None
        self.dados_dir: Path = self._identificar_diretorio_dados()

    def _identificar_diretorio_dados(self) -> Path:
        """Localiza a pasta com os dados da Receita Federal."""
        for d in POSSIVEIS_DIRS_RECEITA:
            if d and os.path.isdir(d):
                logger.info("Diretório de dados da Receita localizado: %s", d)
                return Path(d)
        caminho_padrao = Path("/dados_receita")
        logger.warning(
            "Nenhum diretório de dados encontrado nas opções comuns. Utilizando padrão: %s",
            caminho_padrao,
        )
        return caminho_padrao

    def connect(self) -> Driver:
        auth_token: Optional[Auth] = None
        if self.user or self.password:
            auth_token = (self.user, self.password)  # type: ignore[assignment]
        self._driver = GraphDatabase.driver(self.uri, auth=auth_token)
        self._driver.verify_connectivity()
        logger.info("Conectado com sucesso ao Memgraph em %s", self.uri)
        return self._driver

    def close(self) -> None:
        if self._driver:
            self._driver.close()
            logger.info("Conexão com Memgraph encerrada.")

    def criar_constraints(self) -> None:
        """Cria constraints e índices essenciais no Memgraph."""
        if not self._driver:
            raise RuntimeError("Driver não conectado.")

        queries = [
            "CREATE CONSTRAINT ON (c:CNAE) ASSERT c.codigo IS UNIQUE;",
            "CREATE CONSTRAINT ON (m:Municipio) ASSERT m.codigo_ibge IS UNIQUE;",
            "CREATE CONSTRAINT ON (e:Estado) ASSERT e.uf IS UNIQUE;",
            "CREATE CONSTRAINT ON (r:Regiao) ASSERT r.nome IS UNIQUE;",
            "CREATE CONSTRAINT ON (ri:RegiaoImediata) ASSERT ri.codigo IS UNIQUE;",
            "CREATE CONSTRAINT ON (rint:RegiaoIntermediaria) ASSERT rint.codigo IS UNIQUE;",
            "CREATE INDEX ON :CNAE(codigo_raw);",
            "CREATE INDEX ON :Municipio(nome);",
            "CREATE INDEX ON :RegiaoImediata(nome);",
            "CREATE INDEX ON :RegiaoIntermediaria(nome);",
        ]
        with self._driver.session() as s:
            for q in queries:
                try:
                    s.run(q)
                except Neo4jError as exc:
                    logger.info("Aviso em constraint/índice: %s", exc.message)

    def limpar_banco(self) -> None:
        """Limpa o grafo anterior caso necessário."""
        if not self._driver:
            raise RuntimeError("Driver não conectado.")
        with self._driver.session() as s:
            s.run("MATCH (n) DETACH DELETE n;")
            logger.info("Grafo anterior limpo.")

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

    def processar_e_inserir_brasil_inteiro(self) -> Dict[str, Dict[str, Any]]:
        """
        Carrega todos os 5.570 municípios do Brasil e 27 Estados vinculados às 5 Macrorregiões,
        bem como as Regiões Geográficas Imediatas e Intermediárias oficiais do IBGE.
        Cria o mapeamento siafi_id (código TOM de 4 dígitos da Receita) -> info município.
        """
        logger.info("Carregando malha territorial completa de todos os 5.570 municípios do Brasil e Regiões IBGE...")
        con = duckdb.connect(":memory:")

        # 1. Estados e Macrorregiões
        estados = con.execute("SELECT uf, nome, regiao FROM 'estados.csv'").fetchall()
        batch_estados = [{"uf": r[0], "nome": r[1], "regiao": r[2]} for r in estados]
        q_estados = """
        UNWIND $batch AS item
        MERGE (r:Regiao {nome: item.regiao})
        MERGE (e:Estado {uf: item.uf})
        ON CREATE SET e.nome = item.nome
        ON MATCH SET e.nome = item.nome
        MERGE (e)-[:PERTENCE_A]->(r)
        """
        self.executar_batch(q_estados, batch_estados, "Estados e Regiões")
        logger.info("Inseridos 27 estados vinculados às 5 Macrorregiões.")

        # 2. Todos os Municípios com Regiões Imediatas e Intermediárias do IBGE
        muns = con.execute("""
            SELECT 
                cast(m.codigo_ibge as varchar) as codigo_ibge,
                m.nome,
                cast(m.latitude as double) as lat,
                cast(m.longitude as double) as lon,
                e.uf,
                lpad(trim(cast(m.siafi_id as varchar)), 4, '0') as siafi_tom,
                coalesce(cast(r.codigo_regiao_imediata as varchar), '') as cod_imed,
                coalesce(cast(r.nome_regiao_imediata as varchar), '') as nome_imed,
                coalesce(cast(r.codigo_regiao_intermediaria as varchar), '') as cod_inter,
                coalesce(cast(r.nome_regiao_intermediaria as varchar), '') as nome_inter,
                coalesce(cast(r.nome_microrregiao as varchar), '') as nome_micro,
                coalesce(cast(r.nome_mesorregiao as varchar), '') as nome_meso
            FROM read_csv('municipios.csv') m
            JOIN read_csv('estados.csv') e ON m.codigo_uf = e.codigo_uf
            LEFT JOIN read_csv('regioes_ibge.csv', all_varchar=true) r ON cast(m.codigo_ibge as varchar) = r.codigo_ibge
        """).fetchall()

        batch_muns: List[Dict[str, Any]] = []
        siafi_lookup: Dict[str, Dict[str, Any]] = {}

        for ibge, nome, lat, lon, uf, siafi_tom, cod_imed, nome_imed, cod_inter, nome_inter, nome_micro, nome_meso in muns:
            info = {
                "codigo_ibge": str(ibge),
                "nome": nome,
                "lat": float(lat) if lat is not None else 0.0,
                "lon": float(lon) if lon is not None else 0.0,
                "uf": uf,
                "siafi_tom": siafi_tom,
                "cod_imed": str(cod_imed),
                "nome_imed": str(nome_imed),
                "cod_inter": str(cod_inter),
                "nome_inter": str(nome_inter),
                "nome_micro": str(nome_micro),
                "nome_meso": str(nome_meso),
            }
            batch_muns.append(info)
            siafi_lookup[siafi_tom] = info

        q_muns = """
        UNWIND $batch AS item
        MERGE (m:Municipio {codigo_ibge: item.codigo_ibge})
        ON CREATE SET m.nome = item.nome, m.lat = item.lat, m.lon = item.lon,
                      m.regiao_imediata = item.nome_imed, m.regiao_intermediaria = item.nome_inter,
                      m.microrregiao = item.nome_micro, m.mesorregiao = item.nome_meso
        ON MATCH SET m.nome = item.nome, m.lat = item.lat, m.lon = item.lon,
                     m.regiao_imediata = item.nome_imed, m.regiao_intermediaria = item.nome_inter,
                     m.microrregiao = item.nome_micro, m.mesorregiao = item.nome_meso
        WITH m, item
        MATCH (e:Estado {uf: item.uf})
        MERGE (ri:RegiaoImediata {codigo: item.cod_imed})
        ON CREATE SET ri.nome = item.nome_imed, ri.uf = item.uf
        ON MATCH SET ri.nome = item.nome_imed, ri.uf = item.uf
        MERGE (rint:RegiaoIntermediaria {codigo: item.cod_inter})
        ON CREATE SET rint.nome = item.nome_inter, rint.uf = item.uf
        ON MATCH SET rint.nome = item.nome_inter, rint.uf = item.uf
        MERGE (m)-[:PERTENCE_A]->(ri)
        MERGE (ri)-[:PERTENCE_A]->(rint)
        MERGE (rint)-[:PERTENCE_A]->(e)
        """
        lote_tam = 1000
        for i in range(0, len(batch_muns), lote_tam):
            self.executar_batch(q_muns, batch_muns[i : i + lote_tam], f"Municípios e Regiões IBGE {i}-{i+lote_tam}")

        logger.info("Inseridos %d municípios de todo o território nacional.", len(batch_muns))
        return siafi_lookup

    def calcular_divisas_geograficas(self, siafi_lookup: Dict[str, Dict[str, Any]]) -> None:
        """Gera divisas territoriais entre municípios por proximidade geográfica."""
        logger.info("Calculando divisas e fronteiras territoriais municipais...")
        lista_muns = list(siafi_lookup.values())
        divisas_set: Set[Tuple[str, str, float]] = set()

        # Agrupa por UF para otimizar busca de vizinhos dentro do estado
        muns_por_uf: Dict[str, List[Dict[str, Any]]] = {}
        for m in lista_muns:
            muns_por_uf.setdefault(m["uf"], []).append(m)

        for uf, m_list in muns_por_uf.items():
            for i in range(len(m_list)):
                m1 = m_list[i]
                coord1 = (m1["lat"], m1["lon"])
                vizinhos: List[Tuple[float, Dict[str, Any]]] = []
                for j in range(len(m_list)):
                    if i == j:
                        continue
                    m2 = m_list[j]
                    coord2 = (m2["lat"], m2["lon"])
                    dist = round(geodesic(coord1, coord2).km, 2)
                    if dist <= 45.0:
                        vizinhos.append((dist, m2))
                vizinhos.sort(key=lambda x: x[0])
                for dist, m2 in vizinhos[:4]:
                    ibge_a, ibge_b = sorted([m1["codigo_ibge"], m2["codigo_ibge"]])
                    divisas_set.add((ibge_a, ibge_b, dist))

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
        for i in range(0, len(batch_divisas), 2000):
            self.executar_batch(q_divisas, batch_divisas[i : i + 2000], "FAZ_DIVISA_COM")

        logger.info("Inseridas %d arestas de divisas geodésicas.", len(batch_divisas))

    def carregar_catalogo_cnaes(self) -> Dict[str, Dict[str, str]]:
        """Carrega todos os CNAEs oficiais do arquivo da Receita (*CNAECSV*)."""
        logger.info("Carregando catálogo oficial de CNAEs da Receita Federal...")
        cnae_files = list(self.dados_dir.glob("*CNAECSV*"))
        if not cnae_files:
            # Fallback para Cnaes.zip
            cnae_files = list(self.dados_dir.glob("Cnaes.zip"))
        if not cnae_files:
            raise FileNotFoundError(f"Arquivo de CNAEs não encontrado em {self.dados_dir}")

        cnae_path = str(cnae_files[0])
        logger.info("Lendo arquivo de CNAEs via Python CSV (latin-1): %s", cnae_path)

        cnae_dict: Dict[str, Dict[str, str]] = {}

        def _processar_linhas(iter_linhas: Any) -> None:
            reader = csv.reader(iter_linhas, delimiter=";")
            for row in reader:
                if len(row) >= 2:
                    raw_str = row[0].strip()
                    desc = row[1].strip()
                    fmt = formatar_cnae(raw_str)
                    sec = obter_secao_cnae(raw_str)
                    cnae_dict[raw_str] = {
                        "codigo": fmt,
                        "codigo_raw": raw_str,
                        "descricao": desc,
                        "secao": sec,
                    }

        if cnae_path.endswith(".zip"):
            with zipfile.ZipFile(cnae_path, "r") as zf:
                alvos = [n for n in zf.namelist() if "CNAE" in n.upper()] or zf.namelist()
                with zf.open(alvos[0], "r") as f_inner:
                    texto = io.TextIOWrapper(f_inner, encoding="latin-1", errors="replace")
                    _processar_linhas(texto)
        else:
            with open(cnae_path, mode="r", encoding="latin-1", errors="replace") as f:
                _processar_linhas(f)

        batch_cnaes = list(cnae_dict.values())
        q_cnaes = """
        UNWIND $batch AS item
        MERGE (c:CNAE {codigo: item.codigo})
        ON CREATE SET c.codigo_raw = item.codigo_raw, c.descricao = item.descricao, c.secao = item.secao
        ON MATCH SET c.codigo_raw = item.codigo_raw, c.descricao = item.descricao, c.secao = item.secao
        """
        self.executar_batch(q_cnaes, batch_cnaes, "Nós CNAE")
        logger.info("Inseridos %d nós de CNAE no catálogo oficial.", len(batch_cnaes))
        return cnae_dict

    def _obter_padrao_estabelecimentos(self) -> str:
        """Retorna o padrão de caminho para ler todos os arquivos de estabelecimentos."""
        estab_files = list(self.dados_dir.glob("*ESTABELE*"))
        if estab_files:
            padrao = str(self.dados_dir / "*ESTABELE*")
            logger.info("Localizados %d arquivos descompactados ESTABELE.", len(estab_files))
            return padrao

        zips = list(self.dados_dir.glob("Estabelecimentos*.zip"))
        if zips:
            padrao = str(self.dados_dir / "Estabelecimentos*.zip")
            logger.info("Localizados %d arquivos ZIP de estabelecimentos.", len(zips))
            return padrao

        raise FileNotFoundError(
            f"Nenhum arquivo de estabelecimentos (*ESTABELE* ou Estabelecimentos*.zip) encontrado em {self.dados_dir}"
        )

    def processar_estabelecimentos_nacional(
        self, siafi_lookup: Dict[str, Dict[str, Any]]
    ) -> None:
        """
        Lê em streaming todos os arquivos brutos de Estabelecimentos do Brasil inteiro via DuckDB
        em memória e extrai presenças, métricas de sobrevivência, coocorrências e filiais.
        """
        padrao_estab = self._obter_padrao_estabelecimentos()
        logger.info("Iniciando streaming DuckDB sobre os estabelecimentos: %s", padrao_estab)
        con = duckdb.connect(":memory:")

        # Cria uma VIEW lógica sobre os estabelecimentos brutos da Receita Federal
        con.execute(f"""
            CREATE VIEW view_estabelecimentos AS
            SELECT 
                column00 AS cnpj_basico,
                column01 AS cnpj_ordem,
                column03 AS matriz_filial,
                column05 AS situacao_cadastral,
                column06 AS data_situacao_cadastral,
                column10 AS data_inicio_atividade,
                column11 AS cnae_principal,
                column12 AS cnae_secundaria,
                column19 AS uf,
                lpad(trim(column20), 4, '0') AS siafi_tom
            FROM read_csv(
                '{padrao_estab}',
                delim=';',
                header=false,
                encoding='CP1252',
                all_varchar=true,
                ignore_errors=true
            )
            WHERE length(column11) = 7;
        """)

        # -------------------------------------------------------------
        # 1. Presenças Municipais e Dinâmicas de Sobrevivência (Sebrae 5a)
        # -------------------------------------------------------------
        logger.info("Extraindo presenças e dinâmicas de sobrevivência para todos os municípios...")
        df_presenca = con.execute("""
            SELECT 
                siafi_tom,
                cnae_principal,
                count(*) FILTER (WHERE situacao_cadastral = '02') AS estoque_ativas,
                count(*) FILTER (WHERE data_inicio_atividade >= '20200101') AS abertas_5a,
                count(*) FILTER (WHERE data_inicio_atividade >= '20200101' AND situacao_cadastral = '08') AS encerradas_5a,
                count(*) FILTER (WHERE data_inicio_atividade >= '20240101' AND situacao_cadastral = '02') AS saldo_rec
            FROM view_estabelecimentos
            GROUP BY siafi_tom, cnae_principal
            HAVING count(*) FILTER (WHERE situacao_cadastral = '02') >= 1;
        """).fetchall()
        logger.info("Registros de presença municipal agregados: %d", len(df_presenca))

        q_presenca = """
        UNWIND $batch AS item
        MATCH (c:CNAE {codigo: item.cnae})
        MATCH (m:Municipio {codigo_ibge: item.codigo_ibge})
        MERGE (c)-[r:PRESENTE_EM]->(m)
        SET r.quantidade_empresas_ativas = item.estoque,
            r.estoque_ativas = item.estoque,
            r.abertas_ultimos_5_anos = item.abertas_5a,
            r.encerradas_ultimos_5_anos = item.encerradas_5a,
            r.taxa_sobrevivencia_5_anos = item.taxa_sob,
            r.saldo_recente = item.saldo_rec
        """
        batch: List[Dict[str, Any]] = []
        for tom, cnae_raw, estoque, abertas_5a, encerradas_5a, saldo_rec in df_presenca:
            if tom not in siafi_lookup:
                continue
            ibge = siafi_lookup[tom]["codigo_ibge"]
            ab = int(abertas_5a or 0)
            enc = int(encerradas_5a or 0)
            taxa_sob = round(1.0 - (enc * 1.0 / ab), 4) if ab > 0 else 1.0

            batch.append({
                "cnae": formatar_cnae(str(cnae_raw)),
                "codigo_ibge": ibge,
                "estoque": int(estoque),
                "abertas_5a": ab,
                "encerradas_5a": enc,
                "taxa_sob": taxa_sob,
                "saldo_rec": int(saldo_rec or 0),
            })
            if len(batch) >= 5000:
                self.executar_batch(q_presenca, batch, "PRESENTE_EM")
                batch = []

        if batch:
            self.executar_batch(q_presenca, batch, "PRESENTE_EM")
        logger.info("Concluída a inserção nacional de PRESENTE_EM.")

        # -------------------------------------------------------------
        # 2. Expansão Corporativa Matriz -> Filiais ([:GERA_FILIAL])
        # -------------------------------------------------------------
        logger.info("Extraindo relações agregadas de expansão Matriz -> Filiais...")
        df_filiais = con.execute("""
            WITH matrizes AS (
                SELECT cnpj_basico, cnae_principal AS cnae_matriz
                FROM view_estabelecimentos
                WHERE matriz_filial = '1' AND situacao_cadastral = '02'
            ),
            filiais AS (
                SELECT cnpj_basico, cnae_principal AS cnae_filial
                FROM view_estabelecimentos
                WHERE matriz_filial = '2' AND situacao_cadastral = '02'
            )
            SELECT m.cnae_matriz, f.cnae_filial, count(*) AS frequencia
            FROM matrizes m
            JOIN filiais f ON m.cnpj_basico = f.cnpj_basico
            WHERE m.cnae_matriz != f.cnae_filial
            GROUP BY m.cnae_matriz, f.cnae_filial
            HAVING count(*) >= 20
            ORDER BY count(*) DESC;
        """).fetchall()
        logger.info("Pares de expansão Matriz -> Filiais extraídos: %d", len(df_filiais))

        q_filiais = """
        UNWIND $batch AS item
        MATCH (c1:CNAE {codigo: item.cnae_matriz})
        MATCH (c2:CNAE {codigo: item.cnae_filial})
        MERGE (c1)-[r:GERA_FILIAL]->(c2)
        SET r.frequencia = item.frequencia
        """
        batch_filiais = [
            {
                "cnae_matriz": formatar_cnae(str(r[0])),
                "cnae_filial": formatar_cnae(str(r[1])),
                "frequencia": int(r[2]),
            }
            for r in df_filiais
        ]
        for i in range(0, len(batch_filiais), 2000):
            self.executar_batch(q_filiais, batch_filiais[i : i + 2000], "GERA_FILIAL")
        logger.info("Concluída a inserção nacional de GERA_FILIAL.")

        # -------------------------------------------------------------
        # 3. Coocorrências Globais Nacionais de CNAEs ([:COOCORRE_GERAL])
        # -------------------------------------------------------------
        logger.info("Extraindo coocorrências econômicas nacionais entre atividades...")
        df_coocorrencias = con.execute("""
            WITH pairs AS (
                SELECT 
                    cnae_principal AS cnae_a,
                    trim(unnest(string_split(cnae_secundaria, ','))) AS cnae_b
                FROM view_estabelecimentos
                WHERE situacao_cadastral = '02'
                  AND cnae_secundaria IS NOT NULL
                  AND cnae_secundaria != ''
            ),
            canonical AS (
                SELECT 
                    CASE WHEN cnae_a < cnae_b THEN cnae_a ELSE cnae_b END AS c1,
                    CASE WHEN cnae_a < cnae_b THEN cnae_b ELSE cnae_a END AS c2,
                    count(*) AS frequencia
                FROM pairs
                WHERE length(cnae_b) = 7 AND cnae_a != cnae_b
                GROUP BY c1, c2
                HAVING count(*) >= 30
            )
            SELECT c1, c2, frequencia FROM canonical ORDER BY frequencia DESC;
        """).fetchall()
        logger.info("Total de coocorrências globais extraídas: %d", len(df_coocorrencias))

        q_coocorre = """
        UNWIND $batch AS item
        MATCH (c1:CNAE {codigo: item.cnae_a})
        MATCH (c2:CNAE {codigo: item.cnae_b})
        MERGE (c1)-[r:COOCORRE_GERAL]->(c2)
        SET r.frequencia = item.frequencia
        """
        batch_coocorre = [
            {
                "cnae_a": formatar_cnae(str(r[0])),
                "cnae_b": formatar_cnae(str(r[1])),
                "frequencia": int(r[2]),
            }
            for r in df_coocorrencias
        ]
        for i in range(0, len(batch_coocorre), 5000):
            self.executar_batch(q_coocorre, batch_coocorre[i : i + 5000], "COOCORRE_GERAL")
        logger.info("Concluída a inserção nacional de COOCORRE_GERAL.")


def main() -> None:
    t0 = time.time()
    logger.info("=== INICIANDO PIPELINE DE GRAPHRAG NACIONAL DA RECEITA FEDERAL ===")
    ingestor = ReceitaGraphIngestor(
        uri=MEMGRAPH_URI,
        user=MEMGRAPH_USER,
        password=MEMGRAPH_PASSWORD,
    )

    try:
        ingestor.connect()
        ingestor.criar_constraints()
        ingestor.limpar_banco()

        siafi_lookup = ingestor.processar_e_inserir_brasil_inteiro()
        ingestor.calcular_divisas_geograficas(siafi_lookup)
        ingestor.carregar_catalogo_cnaes()
        ingestor.processar_estabelecimentos_nacional(siafi_lookup)

        t_total = time.time() - t0
        logger.info(
            "=== PIPELINE NACIONAL CONCLUÍDO COM SUCESSO EM %.2f SEGUNDOS ===", t_total
        )
    finally:
        ingestor.close()


if __name__ == "__main__":
    main()
