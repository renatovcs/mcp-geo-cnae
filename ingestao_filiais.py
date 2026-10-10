"""Pipeline ultra-otimizado para extração e inserção de Expansão Matriz -> Filiais ([:GERA_FILIAL]).

Lê em varredura única com projeção estrita os estabelecimentos ativos ('02')
identificando matrizes (1) e filiais (2), cruza os pares de expansão corporativa
e insere no Memgraph:
  - (:CNAE {codigo: cnae_matriz})-[:GERA_FILIAL {frequencia}]->(:CNAE {codigo: cnae_filial})
"""

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
logger = logging.getLogger("ingestao_filiais")

MEMGRAPH_URI = os.getenv("MEMGRAPH_URI", "bolt://memgraph:7687")
MEMGRAPH_USER = os.getenv("MEMGRAPH_USER", "")
MEMGRAPH_PASSWORD = os.getenv("MEMGRAPH_PASSWORD", "")
DADOS_RECEITA_DIR = os.getenv("DADOS_RECEITA_DIR", "/dados_receita")
TEMP_DIR = "/tmp/duckdb_temp_filiais"


def formatar_cnae(cnae_raw: str) -> str:
    limpo = "".join(ch for ch in str(cnae_raw) if ch.isdigit()).zfill(7)
    return f"{limpo[:4]}-{limpo[4]}/{limpo[5:]}"


def executar_batch(driver, query: str, batch: List[Dict[str, Any]], desc: str) -> None:
    if not batch:
        return
    with driver.session() as session:
        session.run(query, batch=batch)
    logger.info("Inserido lote de %d itens para %s.", len(batch), desc)


def main() -> None:
    t0 = time.time()
    logger.info("=== INICIANDO INGESTÃO OTIMIZADA DE MATRIZ -> FILIAIS (GERA_FILIAL) ===")
    logger.info("Memgraph URI: %s", MEMGRAPH_URI)
    logger.info("Dados Receita: %s", DADOS_RECEITA_DIR)

    # 1. Diretório temporário
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
    os.makedirs(TEMP_DIR, exist_ok=True)

    # 2. Conexão Memgraph
    auth = (MEMGRAPH_USER, MEMGRAPH_PASSWORD) if MEMGRAPH_USER or MEMGRAPH_PASSWORD else None
    driver = GraphDatabase.driver(MEMGRAPH_URI, auth=auth)
    driver.verify_connectivity()
    logger.info("Conexão ao Memgraph estabelecida.")

    # 3. DuckDB com limites seguros
    con = duckdb.connect()
    con.execute("PRAGMA max_memory='8GB';")
    con.execute(f"PRAGMA temp_directory='{TEMP_DIR}';")
    con.execute("PRAGMA threads=4;")

    estab_glob = f"{DADOS_RECEITA_DIR}/*ESTABELE*"
    logger.info("Executando varredura única de matrizes e filiais ativas em: %s", estab_glob)

    t_scan = time.time()
    con.execute(f"""
        CREATE TABLE estab_mf AS
        SELECT 
            column00 AS cnpj_basico,
            column03 AS mf,
            column11 AS cnae
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
          AND column03 IN ('1', '2');
    """)
    total_estab = con.execute("SELECT count(*) FROM estab_mf;").fetchone()[0]
    logger.info("Varredura concluída em %.1fs! Total de estabelecimentos ativos carregados: %d", time.time() - t_scan, total_estab)

    # 4. Cruzamento Matriz -> Filiais com cnae diferente
    logger.info("Cruzando padrões de filiais por CNPJ básico...")
    t_join = time.time()
    df_filiais = con.execute("""
        SELECT 
            m.cnae AS cnae_matriz,
            f.cnae AS cnae_filial,
            count(*) AS frequencia
        FROM (SELECT cnpj_basico, cnae FROM estab_mf WHERE mf = '1') m
        JOIN (SELECT cnpj_basico, cnae FROM estab_mf WHERE mf = '2') f
          ON m.cnpj_basico = f.cnpj_basico
        WHERE m.cnae != f.cnae
        GROUP BY m.cnae, f.cnae
        HAVING count(*) >= 10
        ORDER BY frequencia DESC;
    """).fetchall()

    logger.info("Cruzamento finalizado em %.1fs! Pares qualificados de expansão: %d", time.time() - t_join, len(df_filiais))

    # 5. Inserção no Memgraph
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
        executar_batch(driver, q_filiais, batch_filiais[i : i + 2000], "GERA_FILIAL")

    logger.info("Concluída a inserção de GERA_FILIAL no Memgraph.")

    # 6. Limpeza
    con.close()
    driver.close()
    shutil.rmtree(TEMP_DIR, ignore_errors=True)
    logger.info("=== EXPANSÃO MATRIZ-FILIAIS FINALIZADA COM SUCESSO EM %.1f MINUTOS ===", (time.time() - t0) / 60)


if __name__ == "__main__":
    main()
