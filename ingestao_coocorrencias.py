"""Pipeline ultra-otimizado em passo único para cálculo e ingestão de Coocorrências.

Lê os arquivos brutos da Receita Federal em uma única varredura no DuckDB,
agrupa por município e atividade, deriva tanto as coocorrências municipais
quanto as nacionais em memória/disco, e faz o merge no Memgraph:
  - (:CNAE)-[:COOCORRE_GERAL {frequencia}]->(:CNAE)
  - (:CNAE)-[:COOCORRE_EM {codigo_ibge_municipio, frequencia}]->(:CNAE)
"""

import csv
import logging
import os
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List

import duckdb
from neo4j import GraphDatabase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ingestao_coocorrencias")

MEMGRAPH_URI = os.getenv("MEMGRAPH_URI", "bolt://memgraph:7687")
MEMGRAPH_USER = os.getenv("MEMGRAPH_USER", "")
MEMGRAPH_PASSWORD = os.getenv("MEMGRAPH_PASSWORD", "")
DADOS_RECEITA_DIR = os.getenv("DADOS_RECEITA_DIR", "/dados_receita")
TEMP_DIR = "/tmp/duckdb_temp_coocorre"


def formatar_cnae(cnae_raw: str) -> str:
    limpo = "".join(ch for ch in str(cnae_raw) if ch.isdigit()).zfill(7)
    return f"{limpo[:4]}-{limpo[4]}/{limpo[5:]}"


def carregar_municipios_siafi(csv_path: str = "municipios.csv") -> Dict[str, str]:
    """Mapeia SIAFI (4 dígitos) -> codigo_ibge (7 dígitos)."""
    siafi_to_ibge: Dict[str, str] = {}
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_siafi = row.get("siafi_id", "").strip()
            ibge = row.get("codigo_ibge", "").strip()
            if raw_siafi and ibge:
                siafi_pad = raw_siafi.zfill(4)
                siafi_to_ibge[siafi_pad] = ibge
    logger.info("Carregados %d municípios para de/para TOM -> IBGE.", len(siafi_to_ibge))
    return siafi_to_ibge


def executar_batch(driver, query: str, batch: List[Dict[str, Any]], desc: str) -> None:
    if not batch:
        return
    with driver.session() as session:
        session.run(query, batch=batch)
    logger.info("Inserido lote de %d itens para %s.", len(batch), desc)


def main() -> None:
    t0 = time.time()
    logger.info("=== INICIANDO INGESTÃO OTIMIZADA DE COOCORRÊNCIAS ===")
    logger.info("Memgraph URI: %s", MEMGRAPH_URI)
    logger.info("Dados Receita: %s", DADOS_RECEITA_DIR)

    # 1. Prepara diretório temporário
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
    os.makedirs(TEMP_DIR, exist_ok=True)

    # 2. Conexão ao Memgraph
    auth = (MEMGRAPH_USER, MEMGRAPH_PASSWORD) if MEMGRAPH_USER or MEMGRAPH_PASSWORD else None
    driver = GraphDatabase.driver(MEMGRAPH_URI, auth=auth)
    driver.verify_connectivity()
    logger.info("Conexão ao Memgraph verificada com sucesso.")

    with driver.session() as session:
        try:
            session.run("CREATE INDEX ON :CNAE(codigo);")
        except Exception:
            pass

    # 3. Carrega de/para SIAFI -> IBGE
    siafi_map = carregar_municipios_siafi()

    # 4. DuckDB com limites seguros
    con = duckdb.connect()
    con.execute("PRAGMA max_memory='8GB';")
    con.execute(f"PRAGMA temp_directory='{TEMP_DIR}';")
    con.execute("PRAGMA threads=4;")

    estab_glob = f"{DADOS_RECEITA_DIR}/*ESTABELE*"
    logger.info("Configurando leitura dos estabelecimentos brutos: %s", estab_glob)

    # Cria tabela intermediária agregada em passo único
    # Apenas empresas ATIVAS ('02') com CNAE principal e secundárias preenchidas
    t_scan = time.time()
    logger.info("Executando varredura e agregação de coocorrências via DuckDB...")
    con.execute(f"""
        CREATE TABLE coocorrencias_agregadas AS
        WITH raw_pairs AS (
            SELECT 
                lpad(trim(column20), 4, '0') AS siafi_tom,
                column11 AS cnae_a,
                trim(unnest(string_split(column12, ','))) AS cnae_b
            FROM read_csv(
                '{estab_glob}',
                delim=';',
                header=false,
                encoding='CP1252',
                all_varchar=true,
                ignore_errors=true
            )
            WHERE column05 = '02'
              AND length(column11) = 7
              AND column12 IS NOT NULL
              AND column12 != ''
        )
        SELECT 
            siafi_tom,
            CASE WHEN cnae_a < cnae_b THEN cnae_a ELSE cnae_b END AS c1,
            CASE WHEN cnae_a < cnae_b THEN cnae_b ELSE cnae_a END AS c2,
            count(*) AS frequencia
        FROM raw_pairs
        WHERE length(cnae_b) = 7 AND cnae_a != cnae_b
        GROUP BY siafi_tom, c1, c2
        HAVING count(*) >= 2;
    """)
    total_linhas_agg = con.execute("SELECT count(*) FROM coocorrencias_agregadas;").fetchone()[0]
    logger.info(
        "Varredura concluída em %.1fs! Pares agregados gerados: %d",
        time.time() - t_scan,
        total_linhas_agg,
    )

    # -------------------------------------------------------------------------
    # 5. INSERÇÃO NACIONAL: COOCORRE_GERAL
    # -------------------------------------------------------------------------
    logger.info("Extraindo totais nacionais (COOCORRE_GERAL)...")
    df_nacional = con.execute("""
        SELECT c1, c2, sum(frequencia) AS total_freq
        FROM coocorrencias_agregadas
        GROUP BY c1, c2
        HAVING sum(frequencia) >= 15
        ORDER BY total_freq DESC;
    """).fetchall()
    logger.info("Pares nacionais qualificados: %d", len(df_nacional))

    q_geral = """
    UNWIND $batch AS item
    MATCH (c1:CNAE {codigo: item.c1})
    MATCH (c2:CNAE {codigo: item.c2})
    MERGE (c1)-[r:COOCORRE_GERAL]->(c2)
    SET r.frequencia = item.frequencia
    """
    batch_geral = [
        {
            "c1": formatar_cnae(str(r[0])),
            "c2": formatar_cnae(str(r[1])),
            "frequencia": int(r[2]),
        }
        for r in df_nacional
    ]
    for i in range(0, len(batch_geral), 5000):
        executar_batch(driver, q_geral, batch_geral[i : i + 5000], "COOCORRE_GERAL")
    logger.info("Concluída inserção de COOCORRE_GERAL no Memgraph.")

    # -------------------------------------------------------------------------
    # 6. INSERÇÃO MUNICIPAL: COOCORRE_EM
    # -------------------------------------------------------------------------
    logger.info("Extraindo pares municipais (COOCORRE_EM)...")
    df_municipal = con.execute("""
        SELECT siafi_tom, c1, c2, frequencia
        FROM coocorrencias_agregadas
        ORDER BY frequencia DESC;
    """).fetchall()

    q_local = """
    UNWIND $batch AS item
    MATCH (c1:CNAE {codigo: item.c1})
    MATCH (c2:CNAE {codigo: item.c2})
    MERGE (c1)-[r:COOCORRE_EM {codigo_ibge_municipio: item.ibge}]->(c2)
    SET r.frequencia = item.frequencia
    """
    batch_local = []
    for r in df_municipal:
        siafi = str(r[0])
        ibge = siafi_map.get(siafi)
        if not ibge:
            continue
        batch_local.append(
            {
                "ibge": ibge,
                "c1": formatar_cnae(str(r[1])),
                "c2": formatar_cnae(str(r[2])),
                "frequencia": int(r[3]),
            }
        )

    logger.info("Pares municipais qualificados: %d", len(batch_local))
    for i in range(0, len(batch_local), 5000):
        executar_batch(driver, q_local, batch_local[i : i + 5000], "COOCORRE_EM")
    logger.info("Concluída inserção de COOCORRE_EM no Memgraph.")

    # 7. Finalização e limpeza
    con.close()
    driver.close()
    shutil.rmtree(TEMP_DIR, ignore_errors=True)
    logger.info("=== PIPELINE FINALIZADO COM SUCESSO EM %.1f MINUTOS ===", (time.time() - t0) / 60)


if __name__ == "__main__":
    main()
