#!/usr/bin/env bash
# ==============================================================================
# Script de Backup Automático do Memgraph (mcp-geo-cnae)
# Gera dump Cypher compactado com gzip e gerencia rotatividade de backups.
# ==============================================================================
set -euo pipefail

BACKUP_DIR="${HOME}/backups/memgraph"
mkdir -p "${BACKUP_DIR}"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="${BACKUP_DIR}/memgraph_dump_${TIMESTAMP}.cypher.gz"

echo "==> [$(date +'%Y-%m-%d %H:%M:%S')] Iniciando dump do banco Memgraph..."

# Executa DUMP DATABASE via mgconsole e compacta em streaming
docker exec -i memgraph mgconsole -c "DUMP DATABASE;" | gzip -9 > "${BACKUP_FILE}"

echo "==> [$(date +'%Y-%m-%d %H:%M:%S')] Backup concluído com sucesso!"
ls -lh "${BACKUP_FILE}"

# Manter apenas os 4 backups mais recentes (rotatividade para economizar disco)
echo "==> Aplicando política de rotatividade (mantendo últimos 4 backups)..."
ls -t "${BACKUP_DIR}"/memgraph_dump_*.cypher.gz 2>/dev/null | tail -n +5 | xargs -r rm -f --

echo "==> Backups disponíveis em ${BACKUP_DIR}:"
ls -lh "${BACKUP_DIR}"
