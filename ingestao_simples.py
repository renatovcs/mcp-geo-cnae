"""Pipeline otimizado para enriquecimento de PRESENTE_EM com Simples Nacional e MEI.

Lê os arquivos brutos da Receita Federal:
  - F.K03200$W.SIMPLES.CSV.* (Opção pelo Simples e MEI)
  - *ESTABELE* (Estabelecimentos ativos)

Cruza no DuckDB por CNPJ básico (8 dígitos) e atualiza diretamente no Memgraph:
  - (:CNAE)-[r:PRESENTE_EM]->(:Municipio)
    SET r.estoque_mei = ...,
        r.estoque_simples = ...,
        r.taxa_mei = ...
"""

import csv
import logging
import os
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
from neo4j import GraphDatabase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ingestao_simples")

MEMGRAPH_URI = os.getenv("MEMGRAPH_URI", "bolt://memgraph:7687")
MEMGRAPH_USER = os.getenv("MEMGRAPH_USER", "")
MEMGRAPH_PASSWORD = os.getenv("MEMGRAPH_PASSWORD", "")
DADOS_RECEITA_DIR = os.getenv("DADOS_RECEITA_DIR", "/dados_receita")
TEMP_DIR = "/tmp/duckdb_temp_simples"


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


def localizar_arquivo_simples(dados_dir: Path) -> str:
    simples_files = list(dados_dir.glob("*SIMPLES*"))
    if simples_files:
        return str(simples_files[0])
    zips = list(dados_dir.glob("*Simples*.zip"))
    if zips:
        return str(zips[0])
    raise FileNotFoundError(f"Nenhum arquivo do Simples Nacional (*SIMPLES*) encontrado em {dados_dir}")


def executar_batch(driver, query: str, batch: List[Dict[str, Any]], desc: str) -> None:
    if not batch:
        return
    with driver.session() as session:
        session.run(query, batch=batch)


def main() -> None:
    t0 = time.time()
    logger.info("=== INICIANDO ENRIQUECIMENTO DE SIMPLES NACIONAL E MEI ===")
    logger.info("Memgraph URI: %s", MEMGRAPH_URI)
    logger.info("Dados Receita: %s", DADOS_RECEITA_DIR)

    dados_path = Path(DADOS_RECEITA_DIR)
    arquivo_simples = localizar_arquivo_simples(dados_path)
    logger.info("Arquivo Simples localizado: %s", arquivo_simples)

    # 1. Prepara diretório temporário
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
    os.makedirs(TEMP_DIR, exist_ok=True)

    # 2. Conexão ao Memgraph
    auth = (MEMGRAPH_USER, MEMGRAPH_PASSWORD) if MEMGRAPH_USER or MEMGRAPH_PASSWORD else None
    driver = GraphDatabase.driver(MEMGRAPH_URI, auth=auth)
    driver.verify_connectivity()
    logger.info("Conexão ao Memgraph verificada com sucesso.")

    # 3. Carrega de/para SIAFI -> IBGE
    siafi_map = carregar_municipios_siafi()

    # 4. DuckDB com limites seguros de memória e spilling em disco
    con = duckdb.connect()
    con.execute("PRAGMA max_memory='10GB';")
    con.execute(f"PRAGMA temp_directory='{TEMP_DIR}';")
    con.execute("PRAGMA threads=4;")

    # 5. Carrega Simples e MEI ativos
    logger.info("Carregando tabela em memória de empresas ativas no Simples e MEI...")
    t_simples = time.time()
    con.execute(f"""
        CREATE TABLE simples_ativos AS
        SELECT 
            column0 AS cnpj_basico,
            (column1 = 'S' AND (column3 = '00000000' OR column3 > '20261015')) AS is_simples,
            (column4 = 'S' AND (column6 = '00000000' OR column6 > '20261015')) AS is_mei
        FROM read_csv(
            '{arquivo_simples}',
            delim=';',
            header=false,
            all_varchar=true,
            ignore_errors=true
        )
        WHERE (column1 = 'S' AND (column3 = '00000000' OR column3 > '20261015'))
           OR (column4 = 'S' AND (column6 = '00000000' OR column6 > '20261015'));
    """)
    total_simples = con.execute("SELECT count(*) FROM simples_ativos;").fetchone()[0]
    logger.info("Tabela simples_ativos criada com %d registros em %.1fs.", total_simples, time.time() - t_simples)

    # 6. Carrega estabelecimentos ativos com projeção estrita
    estab_glob = f"{DADOS_RECEITA_DIR}/*ESTABELE*"
    logger.info("Executando varredura e projeção de estabelecimentos ativos em: %s", estab_glob)
    t_estab = time.time()
    con.execute(f"""
        CREATE TABLE estab_ativas AS
        SELECT 
            column00 AS cnpj_basico,
            column11 AS cnae,
            lpad(trim(column20), 4, '0') AS siafi_tom
        FROM read_csv(
            '{estab_glob}',
            delim=';',
            header=false,
            encoding='CP1252',
            all_varchar=true,
            ignore_errors=true
        )
        WHERE column05 = '02' AND length(column11) = 7;
    """)
    total_estab = con.execute("SELECT count(*) FROM estab_ativas;").fetchone()[0]
    logger.info("Varredura de estabelecimentos concluída em %.1fs! Total de ativas: %d", time.time() - t_estab, total_estab)

    # 7. Agrupamento municipal cruzado com Simples e MEI
    logger.info("Agrupando estoque ativo, Simples e MEI por (município, CNAE)...")
    t_agg = time.time()
    df_agrupado = con.execute("""
        SELECT 
            e.siafi_tom,
            e.cnae,
            count(*) AS estoque_ativas,
            count(*) FILTER (WHERE s.is_simples) AS estoque_simples,
            count(*) FILTER (WHERE s.is_mei) AS estoque_mei
        FROM estab_ativas e
        LEFT JOIN simples_ativos s ON e.cnpj_basico = s.cnpj_basico
        GROUP BY e.siafi_tom, e.cnae;
    """).fetchall()
    logger.info("Agrupamento concluído em %.1fs! Total de pares (município, CNAE): %d", time.time() - t_agg, len(df_agrupado))

    # 8. Atualização parametrizada das arestas [:PRESENTE_EM] no Memgraph
    logger.info("Iniciando atualização das arestas [:PRESENTE_EM] no Memgraph...")
    q_update = """
    UNWIND $batch AS item
    MATCH (c:CNAE {codigo: item.cnae})-[r:PRESENTE_EM]->(m:Municipio {codigo_ibge: item.codigo_ibge})
    SET r.estoque_mei = item.estoque_mei,
        r.estoque_simples = item.estoque_simples,
        r.taxa_mei = item.taxa_mei
    """
    batch: List[Dict[str, Any]] = []
    lote_tam = 5000
    atualizados = 0

    for tom, cnae_raw, estoque_total, est_simples, est_mei in df_agrupado:
        if tom not in siafi_map:
            continue
        ibge = siafi_map[tom]
        tot = int(estoque_total)
        mei = int(est_mei or 0)
        simples = int(est_simples or 0)
        taxa = round(mei * 1.0 / tot, 4) if tot > 0 else 0.0

        batch.append({
            "cnae": formatar_cnae(str(cnae_raw)),
            "codigo_ibge": ibge,
            "estoque_mei": mei,
            "estoque_simples": simples,
            "taxa_mei": taxa,
        })
        if len(batch) >= lote_tam:
            executar_batch(driver, q_update, batch, "PRESENTE_EM Simples/MEI")
            atualizados += len(batch)
            if atualizados % 50000 == 0:
                logger.info("Progresso: %d arestas PRESENTE_EM enriquecidas...", atualizados)
            batch = []

    if batch:
        executar_batch(driver, q_update, batch, "PRESENTE_EM Simples/MEI")
        atualizados += len(batch)

    logger.info("Atualização concluída com sucesso! Total de arestas enriquecidas: %d em %.1fs", atualizados, time.time() - t0)

    # Limpeza
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
    driver.close()


if __name__ == "__main__":
    main()
