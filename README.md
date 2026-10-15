# MCP Geo-CNAE: Arquitetura GraphRAG Unificada

Servidor MCP (**Model Context Protocol**) em Python integrado ao banco de dados em grafos **Memgraph**, projetado para análises de inteligência de mercado cruzando relações de coocorrência de **CNAEs de empresas ativas na Receita Federal** com a **topologia geográfica de municípios, estados e macrorregiões brasileiras**, além de dinâmicas de sobrevivência empresarial e expansão corporativa matriz-filiais.

> 💡 **Novo**: Veja o [Guia Prático de Perguntas e Casos de Uso](PERGUNTAS_E_CASOS_DE_USO.md) com dezenas de exemplos de perguntas estratégicas de inteligência de mercado prontas para fazer ao assistente.

---

## 🏗️ Arquitetura do Grafo (GraphRAG)

O grafo conecta atividades econômicas, dinâmicas temporais de mercado e topologia física:

- **Nós**:
  - `(:Regiao {nome})`: 5 Macrorregiões oficiais do Brasil (Norte, Nordeste, Centro-Oeste, Sudeste, Sul).
  - `(:Estado {uf, nome})`: Unidades Federativas do Brasil (27 estados indexados).
  - `(:RegiaoIntermediaria {codigo, nome})`: 133 Regiões Geográficas Intermediárias do IBGE (articulação de polos e fluxos econômicos estaduais).
  - `(:RegiaoImediata {codigo, nome})`: 510 Regiões Geográficas Imediatas do IBGE (rede urbana estruturada em torno de centros urbanos locais).
  - `(:Municipio {codigo_ibge, nome, lat, lon, regiao_imediata, regiao_intermediaria, microrregiao, mesorregiao})`: Todos os 5.570 municípios do Brasil com coordenadas geodésicas e metadados regionais completos do IBGE.
  - `(:CNAE {codigo, codigo_raw, descricao, secao})`: 1.359 subclasses econômicas oficiais da Receita Federal classificadas nas seções A até U.
- **Arestas**:
  - `(:Municipio)-[:PERTENCE_A]->(:RegiaoImediata)-[:PERTENCE_A]->(:RegiaoIntermediaria)-[:PERTENCE_A]->(:Estado)-[:PERTENCE_A]->(:Regiao)`: Hierarquia territorial e regional multinível do IBGE.
  - `(:Municipio)-[:FAZ_DIVISA_COM {distancia_km}]-(:Municipio)`: Relação de fronteira física real com cálculo geodésico de distância via `geopy.distance.geodesic`.
  - `(:CNAE)-[:COOCORRE_GERAL {frequencia}]-(:CNAE)`: Coocorrências entre atividade principal e secundárias de empresas ativas em escala nacional.
  - `(:CNAE)-[:COOCORRE_EM {frequencia, codigo_ibge_municipio}]-(:CNAE)`: Coocorrência cruzada restrita ao município.
  - `(:CNAE)-[:GERA_FILIAL {frequencia}]->(:CNAE)`: Relação agregada entre a atividade principal da Matriz e as atividades abertas como Filiais (padrões corporativos de desdobramento e verticalização).
  - `(:CNAE)-[:PRESENTE_EM {estoque_ativas, estoque_mei, estoque_simples, taxa_mei, abertas_ultimos_5_anos, encerradas_ultimos_5_anos, taxa_sobrevivencia_5_anos, saldo_recente}]->(:Municipio)`: Densidade territorial, porte predominante (MEI vs Simples vs Geral) e indicadores de dinamismo e sobrevivência setorial.

---

## 📁 Processamento Direto dos Dados Brutos da Receita Federal

O pipeline foi projetado para **não depender de arquivos intermediários em disco (.duckdb ou .parquet)**. O processamento é realizado por **streaming paralelo em memória RAM** (`:memory:`) via DuckDB sobre a pasta original dos Dados Abertos da Receita Federal.

### Estrutura dos Arquivos em `~/dados_receita` (Servidor OCI):

```
dados_receita/
├── K3241.K03200Y0.D60912.ESTABELE ... Y9.D60912.ESTABELE (10 arquivos de Estabelecimentos)
├── K3241.K03200Y0.D60912.EMPRECSV ... Y9.D60912.EMPRECSV (10 arquivos de Empresas)
├── F.K03200$Z.D60912.CNAECSV                             (Catálogo de CNAEs)
├── F.K03200$Z.D60912.NATJUCSV                            (Naturezas Jurídicas)
├── F.K03200$Z.D60912.MOTICSV                             (Motivos de Situação)
└── F.K03200$W.SIMPLES.CSV.D60912                         (Opção pelo Simples e MEI)
```

### Mapeamento Preciso SIAFI/TOM -> IBGE:
Na tabela de estabelecimentos da Receita (`ESTABELE`), a coluna 21 armazena o código municipal da Receita Federal (SIAFI / TOM). O arquivo `municipios.csv` contém a correspondência exata de todos os **5.570 municípios brasileiros**:

$$\text{ESTABELE.municipio (TOM)} \xrightarrow{\text{LPAD 4 dígitos}} \text{municipios.csv.siafi_id} \longrightarrow \text{codigo\_ibge, lat, lon, UF}$$

Isso elimina qualquer ambiguidade de homônimos ou divergências de acentuação entre municípios brasileiros.

---

## 📊 Diretrizes Metodológicas e Recomendações de Ação

| Tópico Analisado | Decisão Técnica | Justificativa e Padrão da Indústria |
|---|---|---|
| **Regime Tributário (Simples & MEI)** | **Propriedades na aresta `[:PRESENTE_EM]` (`estoque_mei`, `estoque_simples`, `taxa_mei`)** | Evita o antipattern de *Supernodes* ou explosão de 50M de nós de CNPJ. Permite filtragem precisa para soluções B2B e cálculo de mercado endereçável (TAM) separando autônomos (MEI) de empresas estruturadas com overhead zero de travessia. |
| **Linha Temporal no Grafo** | **Propriedades na aresta (Sem nós de meses/anos)** | Evita o antipattern clássico de *Supernodes* (nós densos com centenas de milhares de conexões que travam traversals e estouram tokens no GraphRAG). Agregações numéricas temporais são preservadas como propriedades. |
| **Sentido da Aresta para Ativa/Inativa** | **Rejeitado (Antipattern)** | Inverter a direção da aresta para denotar encerramento quebra queries Cypher direcionadas e induz alucinações em LLMs. A semântica é expressa por propriedades (`estoque_ativas`, `encerradas_ultimos_5_anos`) ou arestas tipadas. |
| **Cálculo da Taxa de Mortalidade** | **Safra de 5 anos (Metodologia Sebrae/IBGE)** | Comparar empresas encerradas de 30 anos atrás com ativas hoje gera viés de sobrevivência severo. Adotou-se a taxa de sobrevivência em ciclo crítico de 5 anos ($1 - \frac{\text{baixadas 5a}}{\text{abertas 5a}}$) e saldo anual recente. |
| **Matriz -> Filiais** | **Aresta Agregada `[:GERA_FILIAL]`** | Mapeia os padrões de expansão setorial em nível macroeconômico sem inflar a memória do banco com 50M de nós individuais de CNPJs. |
| **Empresas Baixadas** | **Apenas CNAE Principal** | Reduz sobrecarga e ruído de secundárias em entidades extintas, focando na atividade core que pautou o encerramento. |
| **Hierarquia Geográfica** | **Aprovado (`RegiaoImediata`, `RegiaoIntermediaria`, `Estado`, `Regiao`)** | Modela formalmente as divisões intraestaduais do IBGE (510 Regiões Imediatas e 133 Intermediárias) além de estados e macrorregiões, permitindo análises por polos econômicos regionais sem ruído. |

---

## 🛠️ Ferramentas (Tools) do Servidor MCP

| Ferramenta | Parâmetros | Descrição |
|---|---|---|
| `buscar_coocorrencia_cnaes_global` | `codigo_cnae: str`, `limite: int = 20` | Retorna os CNAEs que mais coocorrem no Brasil com a atividade em empresas ativas (aceita `'6201-5/01'` ou `'6201501'`). |
| `buscar_coocorrencia_cnaes_municipio` | `codigo_cnae: str`, `codigo_ibge: str`, `limite: int = 20` | Retorna as coocorrências econômicas restritas ao município informado (por código IBGE ou nome). |
| `buscar_malha_municipal_vizinha` | `codigo_ibge: str` | Retorna a topologia de fronteiras de um município, distâncias geodésicas dos municípios limítrofes, Região Imediata, Região Intermediária, Estado e Macrorregião. |
| `buscar_perfil_economico_municipio` | `codigo_ibge: str`, `top_cnaes: int = 10` | Retorna o ranking das atividades com maior concentração de empresas ativas, Região Imediata e Intermediária do IBGE e indicadores de dinâmica/sobrevivência. |
| `buscar_perfil_economico_regiao_ibge` | `nome_regiao: str`, `top_cnaes: int = 10` | Agrega e sumariza os principais setores econômicos e empresas ativas de toda uma Região Geográfica Imediata ou Intermediária do IBGE (ex: `'Curitiba'`, `'Campinas'`, `'Ponta Grossa'`). |
| `buscar_dinamica_sobrevivencia_cnae` | `codigo_cnae: str`, `codigo_ibge: str` | Retorna a análise setorial aprofundada: estoque ativo, abertas e baixadas em 5 anos, taxa de sobrevivência (Sebrae/IBGE), saldo recente e diagnóstico de viabilidade. |
| `buscar_expansao_filiais_cnae` | `codigo_cnae: str`, `limite: int = 10` | Retorna as atividades econômicas mais frequentes abertas como filiais a partir da matriz daquele CNAE (`[:GERA_FILIAL]`). |
| `buscar_visao_macro_regiao` | `nome_regiao: str` | Retorna a composição de estados e municípios mapeados de uma das 5 macrorregiões do Brasil (ex: `'Sul'`, `'Sudeste'`). |

---

## 🚀 Como Executar no Servidor OCI (Produção)

No servidor de produção na Oracle Cloud Infrastructure (`ubuntu@141.148.38.30`):

### 1. Conectar via SSH e acessar o projeto

```bash
ssh ubuntu@141.148.38.30
cd ~/mcp-geo-cnae
```

### 2. Iniciar o Memgraph e Memgraph Lab

```bash
docker compose up -d memgraph memgraph-lab
```

* **Memgraph Bolt**: `bolt://localhost:7687`
* **Memgraph Lab (Interface Visual Web)**: [http://141.148.38.30:3000](http://141.148.38.30:3000)

### 3. Executar o Container de Ingestão Nacional (Brasil Inteiro)

O container `geo-cnae-ingestor` lê diretamente a pasta `/home/ubuntu/dados_receita` montada em `/dados_receita:ro`. O script lê em streaming os arquivos `*.ESTABELE` e `*.CNAECSV`, calcula as métricas em RAM e popula o Memgraph:

```bash
docker compose run --rm geo-cnae-ingestor
```

> **Dica**: Acompanhe o log detalhado no terminal. A operação é executada com batches otimizados `UNWIND` e encerra o container automaticamente ao concluir.

### 4. Iniciar o Servidor MCP (Modo HTTP Persistente)

O servidor MCP opera em modo `streamable-http` na porta **8000** interna (mapeada para a porta **8006** do host).
*(Nota: O modo `streamable-http` evita o encerramento prematuro que ocorreria no modo `stdio` dentro do Docker sem TTY interativo).*

```bash
docker compose up -d geo-cnae-server
```

Para inspecionar logs do servidor:
```bash
docker compose logs -f geo-cnae-server
```

---

## 💻 Como Executar em Ambiente Local / Desenvolvimento

### 1. Subir os containers do Memgraph
```bash
docker compose up -d memgraph memgraph-lab
```

### 2. Instalar Dependências e Executar Testes Automatizados
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m unittest tests/test_mcp_geo_cnae.py
```

### 3. Ingestão Sintética Rápida (Mock de Validação)
```bash
python ingestao.py
```

### 4. Iniciar Servidor MCP Local via Terminal
```bash
python server.py
```

---

## 🔌 Configuração no Cliente MCP (Claude Desktop / Cursor / Antigravity)

### Opção 1: Conexão Direta HTTP / Streamable (Recomendado para Servidor Remoto)
No seu cliente compatível com MCP (Antigravity, Cursor, etc.), configure a URL HTTP direta:
```json
{
  "mcpServers": {
    "geo-cnae-remote": {
      "url": "http://141.148.38.30:8006/mcp"
    }
  }
}
```

### Opção 2: Conexão Local via stdio:
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

### Opção 3: Conexão com Servidor OCI via SSH Bridge:
```json
{
  "mcpServers": {
    "geo-cnae-oci": {
      "command": "ssh",
      "args": [
        "ubuntu@141.148.38.30",
        "docker exec -i mcp-geo-cnae-server python server.py"
      ]
    }
  }
}
```

---

## 🔍 Guia Completo do Memgraph Lab (Interface Web)

Acesse `http://141.148.38.30:3000` (ou `http://localhost:3000` se estiver rodando localmente).

### ⚠️ Regras Cruciais para Consultas Cypher

1. **Direção da Aresta `PRESENTE_EM`**:
   - No modelo, o CNAE está presente no município:
     `(:CNAE)-[:PRESENTE_EM]->(:Municipio)`
   - Se você inverter a seta (`(:Municipio)-[:PRESENTE_EM]->(:CNAE)`), o Cypher **não encontrará nenhuma aresta e retornará vazio (`No data returned`)**. Você também pode usar a aresta sem setas: `(:Municipio)-[:PRESENTE_EM]-(:CNAE)`.

2. **Nomes Exatos das Propriedades**:
   - **Nó `(:CNAE)`**:
     - `codigo` (ex: `"6201-5/01"`)
     - `descricao` (ex: `"Desenvolvimento de programas de computador sob encomenda"`) — *não use `denominacao`*.
     - `secao` (ex: `"J"`)
   - **Nó `(:Municipio)`**:
     - `nome` (ex: `"São Paulo"`, `"Florianópolis"`)
     - `codigo_ibge` (ex: `"3550308"`)
     - `lat`, `lon` (coordenadas geodésicas)
     - `regiao_imediata`, `regiao_intermediaria`, `microrregiao`, `mesorregiao`
   - **Aresta `[:PRESENTE_EM]`**:
     - `quantidade_empresas_ativas` (ou `estoque_ativas`): volume total de empresas ativas.
     - `estoque_mei`: quantidade de Microempreendedores Individuais.
     - `estoque_simples`: quantidade de optantes pelo Simples Nacional.
     - `taxa_mei`: proporção de MEIs sobre o total (de `0.0` a `1.0`).
     - `abertas_ultimos_5_anos`, `encerradas_ultimos_5_anos`, `saldo_recente`
     - `taxa_sobrevivencia_5_anos`: taxa metodológica Sebrae/IBGE ($1 - \frac{\text{baixadas}}{\text{abertas}}$).

3. **Como Visualizar os Nós e Arestas Graficamente**:
   - No editor Cypher, execute uma consulta que retorne as entidades inteiras (ex: `RETURN m, p, c LIMIT 25;`).
   - Logo abaixo da área de texto, a aba **`Graph results`** ficará clicável. Clique nela para abrir o canvas interativo 2D/3D com nós coloridos, zoom e drag-and-drop.

---

### 🚨 Nota sobre a aba `GraphChat (EXPERIMENTAL)`

O Memgraph Lab possui uma aba chamada **GraphChat** que utiliza modelos LLM para gerar Cypher automaticamente.
- Se ao enviar uma mensagem você receber o erro:
  `{"error": {"code": 400, "message": "Function call is missing a thought_signature in functionCall parts..."}}`
- **Motivo**: Modelos recentes do Google Gemini (como a série Gemini 2.x com raciocínio/*thinking*) exigem o envio de um token de `thought_signature` nas chamadas de função (`run-cypher-query`), o qual a versão atual do Memgraph Lab ainda não implementa.
- **Solução**: No seletor de modelos do GraphChat, escolha um modelo sem thinking forçado (como **Gemini 1.5 Flash**, **Gemini 1.5 Pro**, **GPT-4o** ou **Claude 3.5 Sonnet**) ou utilize diretamente a aba oficial **`<> Query execution`**.

---

### 💡 Consultas Cypher Prontas para Copiar e Colar

#### 1. Top 10 Atividades e Taxa de MEI em São Paulo
```cypher
MATCH (c:CNAE)-[p:PRESENTE_EM]->(m:Municipio {nome: "São Paulo"})
RETURN c.codigo AS cnae, 
       c.descricao AS atividade, 
       p.quantidade_empresas_ativas AS total_empresas, 
       p.estoque_mei AS mei, 
       p.taxa_mei AS taxa_mei
ORDER BY p.quantidade_empresas_ativas DESC
LIMIT 10;
```

#### 2. Visualização Gráfica Interativa (Florianópolis)
```cypher
MATCH (c:CNAE)-[p:PRESENTE_EM]->(m:Municipio {nome: "Florianópolis"})
RETURN m, p, c
LIMIT 25;
```
*(Clique na aba `Graph results` para ver o grafo visual).*

#### 3. Dinâmica de sobrevivência setorial em Curitiba:
```cypher
MATCH (c:CNAE {codigo: "6201-5/01"})-[r:PRESENTE_EM]->(m:Municipio {nome: "Curitiba"})
MATCH (m)-[:PERTENCE_A*1..3]->(e:Estado)-[:PERTENCE_A]->(reg:Regiao)
RETURN m.nome AS municipio, e.uf AS uf, reg.nome AS regiao,
       r.quantidade_empresas_ativas AS ativas,
       r.estoque_mei AS mei,
       r.estoque_simples AS simples,
       r.taxa_sobrevivencia_5_anos AS taxa_sobrevivencia;
```

#### 4. Ramos mais comuns de expansão corporativa Matriz -> Filiais:
```cypher
MATCH (matriz:CNAE {codigo: "6201-5/01"})-[r:GERA_FILIAL]->(filial:CNAE)
RETURN filial.codigo AS filial_cnae, filial.descricao AS ramo_filial, r.frequencia AS ocorrencias
ORDER BY r.frequencia DESC
LIMIT 10;
```

#### 5. Navegação territorial e municípios vizinhos:
```cypher
MATCH (m:Municipio {nome: "São Paulo"})-[r:FAZ_DIVISA_COM]-(viz:Municipio)
RETURN m.nome AS origem, viz.nome AS vizinho, r.distancia_km AS distancia_km
ORDER BY r.distancia_km ASC;
```

#### 6. Traversal da hierarquia intraestadual do IBGE:
```cypher
MATCH (m:Municipio {nome: "Castro"})-[:PERTENCE_A]->(ri:RegiaoImediata)-[:PERTENCE_A]->(rint:RegiaoIntermediaria)-[:PERTENCE_A]->(e:Estado)-[:PERTENCE_A]->(reg:Regiao)
RETURN m.nome AS municipio,
       ri.nome AS regiao_imediata,
       rint.nome AS regiao_intermediaria,
       e.uf AS estado,
       reg.nome AS macrorregiao;
```

