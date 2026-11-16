FROM python:3.15-slim

WORKDIR /app

# Instala dependências do sistema necessárias para compilação e rede
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copia e instala dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia código-fonte e tabelas geográficas oficiais
COPY . .

# Cria usuário não-privilegiado para execução segura (OWASP A05)
RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app

USER appuser

# Variáveis padrão de ambiente
ENV PYTHONUNBUFFERED=1 \
    MEMGRAPH_URI=bolt://memgraph:7687 \
    MEMGRAPH_USER="" \
    MEMGRAPH_PASSWORD="" \
    DADOS_RECEITA_DIR=/dados_receita \
    MCP_TRANSPORT=streamable-http \
    MCP_HOST=0.0.0.0 \
    MCP_PORT=8000

EXPOSE 8000

# Comando padrão inicia o servidor MCP
CMD ["python", "server.py"]
