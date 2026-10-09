# MCP Geo-CNAE: Arquitetura GraphRAG Unificada

Servidor MCP (**Model Context Protocol**) em Python integrado ao banco de dados em grafos **Memgraph** (MAGE), projetado para análises de inteligência de mercado cruzando relações de coocorrência de **CNAEs de empresas ativas na Receita Federal** com a **topologia geográfica de municípios e estados brasileiros**.

---

## 🏗️ Arquitetura do Grafo (GraphRAG)

O grafo conecta atividades econômicas, dinâmica de mercado e topologia física:

- **Nós**:
  - `(:Estado {uf, nome})`: Unidades Federativas do Brasil (27 estados indexados).
  - `(:Municipio {codigo_ibge, nome, lat, lon})`: Municípios com coordenadas geodésicas.
  - `(:CNAE {codigo, codigo_raw, descricao, secao})`: 1.359 subclasses econômicas oficiais da Receita Federal classificadas nas seções A até U.
- **Arestas**:
  - `(:Municipio)-[:PERTENCE_A]->(:Estado)`: Localização hierárquica.
  - `(:Municipio)-[:FAZ_DIVISA_COM {distancia_km}]-(:Municipio)`: Relação de fronteira física real com cálculo geodésico de distância via `geopy.distance.geodesic`.
  - `(:CNAE)-[:COOCORRE_GERAL {frequencia}]-(:CNAE)`: Coocorrências entre atividade principal e secundárias de empresas ativas em escala nacional.
  - `(:CNAE)-[:COOCORRE_EM {frequencia, codigo_ibge_municipio}]-(:CNAE)`: Coocorrência cruzada restrita ao município.
  - `(:CNAE)-[:PRESENTE_EM {quantidade_empresas_ativas}]->(:Municipio)`: Densidade territorial de empresas ativas por atividade.

---

## 📊 Carga Real de Dados da Receita Federal

O pipeline processa o dataset de **7,16 milhões de empresas ativas** (`/mnt/e/anotae/data/receita/processed/empresas_ativas.parquet`):

- **Tempo de agregação e ingestão**: ~130 segundos utilizando DuckDB + batches `UNWIND` no Memgraph.
- **27 Estados** e **138 Municípios** estratégicos (RMSP, RMRJ, RMC e Campos Gerais).
- **1.359 CNAEs** com descrições oficiais da Receita Federal.
- **44.332 arestas** `COOCORRE_GERAL`.
- **116.060 arestas** `COOCORRE_EM`.
- **74.843 arestas** `PRESENTE_EM`.
- **342 arestas** `FAZ_DIVISA_COM` com distâncias calculadas.

---

## 🚀 Como Executar

### 1. Subir o Memgraph MAGE e Memgraph Lab

```bash
docker compose up -d
```

- **Bolt**: `bolt://localhost:7687`
- **Memgraph Lab**: [http://localhost:3000](http://localhost:3000)

### 2. Montar o Drive do Windows no WSL (se necessário)

Caso o drive externo `E:` não esteja montado:

```bash
sudo mkdir -p /mnt/e
sudo mount -t drvfs E: /mnt/e -o uid=$(id -u),gid=$(id -g)
```

### 3. Instalar Dependências

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4. Executar a Ingestão

- **Carga Completa Real (7,16M empresas ativas da Receita)**:
  ```bash
  python ingestao_receita.py
  ```
- **Carga Sintética Leve (Mock rápido de validação)**:
  ```bash
  python ingestao.py
  ```

### 5. Executar os Testes Automatizados

```bash
python -m unittest tests/test_mcp_geo_cnae.py
```

### 6. Executar o Servidor MCP

```bash
python server.py
```

---

## 🛠️ Ferramentas (Tools) do Servidor MCP

| Ferramenta | Parâmetros | Descrição |
|---|---|---|
| `buscar_coocorrencia_cnaes_global` | `codigo_cnae: str`, `limite: int = 20` | Retorna os CNAEs que mais coocorrem no Brasil com a atividade em empresas ativas (aceita `'6201-5/01'` ou `'6201501'`). |
| `buscar_coocorrencia_cnaes_municipio` | `codigo_cnae: str`, `codigo_ibge: str`, `limite: int = 20` | Retorna as coocorrências econômicas restritas ao município informado (por código IBGE ou nome). |
| `buscar_malha_municipal_vizinha` | `codigo_ibge: str` | Retorna a topologia de fronteiras de um município e as distâncias geodésicas em km dos municípios limítrofes. |
| `buscar_perfil_economico_municipio` | `codigo_ibge: str`, `top_cnaes: int = 10` | Retorna o ranking das atividades com maior concentração de empresas ativas e contagem total do município. |

---

## 🔌 Configuração no Cliente MCP (Claude Desktop / Cursor / Antigravity)

```json
{
  "mcpServers": {
    "geo-cnae": {
      "command": "/home/renatovcs/mcp-geo-cnae/.venv/bin/python",
      "args": ["/home/renatovcs/mcp-geo-cnae/server.py"],
      "env": {
        "MEMGRAPH_URI": "bolt://localhost:7687",
        "MEMGRAPH_USER": "",
        "MEMGRAPH_PASSWORD": ""
      }
    }
  }
}
```
