# 🧠 Guia Prático de Perguntas e Casos de Uso (MCP Geo-CNAE)

Este guia reúne exemplos reais de **perguntas estratégicas de alto valor** que você pode fazer diretamente ao seu assistente de IA (Claude Desktop, Cursor, Antigravity, etc.) conectado a este servidor MCP.

O diferencial deste MCP é cruzar **50 milhões de CNPJs da Receita Federal** com a **topologia geográfica oficial do IBGE**, respondendo instantaneamente a perguntas que exigiriam dias de trabalho de analistas ou consultas SQL impossíveis.

---

## 🎯 1. Expansão Comercial e Escolha de Localização (Franquias & Redes)

*Perfeito para diretores de expansão, franqueadores, imobiliárias comerciais e novos negócios que precisam decidir onde investir.*

### 💬 "Quero abrir uma clínica odontológica no interior de SP. Como está a concorrência e a sobrevivência em Ribeirão Preto e cidades vizinhas?"
* **Ferramentas acionadas:** `buscar_perfil_economico_municipio`, `buscar_malha_municipal_vizinha`, `buscar_dinamica_sobrevivencia_cnae`.
* **O que o Grafo responde:**
  - O estoque atual de dentistas e clínicas ativas em Ribeirão Preto.
  - A lista de todos os municípios vizinhos limítrofes (`FAZ_DIVISA_COM`) com a distância exata em km (ex: Sertãozinho a 21 km, Cravinhos a 22 km).
  - A taxa de sobrevivência de 5 anos (Sebrae) e o saldo recente em cada município, apontando se o mercado está saturado ou em expansão.

### 💬 "Quais são as 5 cidades que fazem fronteira com Maringá (PR) e qual delas tem a maior concentração de empresas de transporte rodoviário de cargas?"
* **Ferramentas acionadas:** `buscar_malha_municipal_vizinha`, `buscar_dinamica_sobrevivencia_cnae`.
* **O que o Grafo responde:**
  - Identifica Sarandi, Paiçandu, Floresta, etc., e analisa a densidade do CNAE `4930-2/02` em cada um, revelando polos logísticos satélites.

---

## 🏢 2. Inteligência Corporativa e Verticalização (Matriz $\rightarrow$ Filiais)

*Perfeito para consultorias de estratégia, analistas de M&A e fundos de private equity avaliando o modelo de expansão de grandes setores.*

### 💬 "Quando uma indústria de laticínios expande e abre filiais pelo Brasil, quais atividades ela mais costuma registrar?"
* **Ferramentas acionadas:** `buscar_expansao_filiais_cnae(codigo_cnae='1052-0/00')`.
* **O que o Grafo responde:**
  - Percorre as relações `[:GERA_FILIAL]` geradas a partir dos CNPJs básicos da Receita Federal.
  - Mostra com exatidão a frequência com que indústrias abrem filiais de centros de distribuição (`5211-7/99`), transporte próprio (`4930-2/02`) ou comércio atacadista de alimentos (`4631-1/00`).

### 💬 "Quais são os desdobramentos de filiais mais comuns abertos por empresas de desenvolvimento de software no Brasil?"
* **Ferramentas acionadas:** `buscar_expansao_filiais_cnae(codigo_cnae='6201-5/01')`.
* **O que o Grafo responde:**
  - Revela se empresas de software costumam abrir filiais focadas em suporte técnico (`6209-1/00`), consultoria em gestão (`7020-4/00`) ou tratamento de dados (`6311-9/00`).

---

## 💡 3. Estratégia de Produto, Cross-Sell e Parcerias (CNAEs Correlatos)

*Perfeito para software houses B2B, contabilidades e consultorias desenhando novos produtos ou identificando ofertas casadas.*

### 💬 "Quem abre um escritório de contabilidade no Brasil, que outras atividades secundárias costuma colocar no contrato social?"
* **Ferramentas acionadas:** `buscar_coocorrencia_cnaes_global(codigo_cnae='6920-6/01')`.
* **O que o Grafo responde:**
  - Ranking nacional das atividades mais associadas a contadores: Consultoria em gestão empresarial (`7020-4/00`), treinamento e cursos (`8599-6/04`), perícia contábil e cobrança.
  - *Aplicação prática:* Um desenvolvedor de software para contadores descobre quais módulos extras precisa incluir no sistema.

### 💬 "Existe diferença entre os serviços associados a empresas de TI em Florianópolis versus Campinas?"
* **Ferramentas acionadas:** `buscar_coocorrencia_cnaes_municipio(codigo_cnae='6201-5/01', codigo_ibge=...)`.
* **O que o Grafo responde:**
  - Compara as coocorrências locais (`[:COOCORRE_EM]`), mostrando as vocações específicas de cada ecossistema (ex: maior ligação com hardware/telecom em Campinas e maior ligação com marketing/SaaS em Florianópolis).

---

## 📉 4. Análise de Risco de Crédito e Dinâmica de Sobrevivência (Fintechs & Bancos)

*Perfeito para instituições financeiras, cooperativas de crédito e analistas de risco definindo taxas e limites setoriais.*

### 💬 "Qual é a taxa real de sobrevivência de bares e restaurantes em Curitiba nos primeiros 5 anos de vida?"
* **Ferramentas acionadas:** `buscar_dinamica_sobrevivencia_cnae(codigo_cnae='5611-2/01', codigo_ibge='4106902')`.
* **O que o Grafo responde:**
  - Total de estabelecimentos ativos hoje.
  - Quantas empresas abriram nos últimos 5 anos versus quantas fecharam.
  - Taxa de sobrevivência oficial (metodologia Sebrae/IBGE: $1 - \frac{\text{baixadas}}{\text{abertas}}$) e saldo anual recente de novos estabelecimentos.

### 💬 "O setor de comércio varejista de vestuário em Belo Horizonte está em momento de retração ou expansão?"
* **Ferramentas acionadas:** `buscar_dinamica_sobrevivencia_cnae(codigo_cnae='4781-4/00', codigo_ibge='3106200')`.
* **O que o Grafo responde:**
  - Diagnóstico automatizado: saldo recente de abertura, relação de mortalidade e curva de crescimento da atividade na praça.

---

## 🗺️ 5. Políticas Públicas, Polos Regionais e Macroeconômicos (IBGE)

*Perfeito para prefeituras, secretarias de desenvolvimento, Sebrae e federações industriais.*

### 💬 "Qual é o perfil econômico consolidado de toda a Região Geográfica Imediata de Chapecó (SC)?"
* **Ferramentas acionadas:** `buscar_perfil_economico_regiao_ibge(nome_regiao='Chapecó')`.
* **O que o Grafo responde:**
  - Agrega todos os municípios que compõem a rede urbana da região imediata do IBGE.
  - Apresenta o Top 10 dos setores mais fortes e o volume de empresas ativas somadas em todo o polo agroindustrial.

### 💬 "Quais são os principais polos econômicos da Macrorregião Sul e a composição de estados?"
* **Ferramentas acionadas:** `buscar_visao_macro_regiao(nome_regiao='Sul')`.
* **O que o Grafo responde:**
  - Panorama estruturado com total de estados, divisões territoriais e conexões dos municípios com as regiões intermediárias e imediatas.

---

## 📊 6. Qualificação de Porte, MEI e Simples Nacional (Inteligência B2B & Mercado Endereçável)

*Perfeito para empresas SaaS B2B, bancos, consultorias e indústrias que precisam filtrar autônomos (MEI) e focar em empresas estruturadas (Simples ou Regime Geral).*

### 💬 "No setor de desenvolvimento de software em Florianópolis, quanto do mercado é MEI (unipessoal) versus empresas estruturadas?"
* **Ferramentas acionadas:** `buscar_dinamica_sobrevivencia_cnae(codigo_cnae='6201-5/01', codigo_ibge='4205407')`.
* **O que o Grafo responde:**
  - Estoque total de empresas ativas no setor.
  - Estoque de MEIs, estoque de empresas optantes pelo Simples Nacional e empresas do Regime Geral (Lucro Presumido/Real).
  - Taxa percentual de MEI e diagnóstico automatizado de maturidade do polo (`Predominância de empresas estruturadas` vs `Misto` vs `Predominância MEI`).

### 💬 "Quais são as principais atividades econômicas de Campinas e qual o grau de 'MEIzação' de cada uma delas?"
* **Ferramentas acionadas:** `buscar_perfil_economico_municipio(codigo_ibge='3509502')`.
* **O que o Grafo responde:**
  - Total geral de empresas ativas mapeadas em Campinas.
  - Total e proporção geral de MEIs e optantes pelo Simples Nacional no município.
  - Para cada um dos top CNAEs: quantidade de MEIs, quantidade de optantes pelo Simples e a `taxa_mei_percentual`.

---

## 📋 Tabela Resumo: Pergunta $\rightarrow$ Ferramenta

| Tipo de Decisão | Exemplo de Pergunta | Ferramenta MCP |
|---|---|---|
| **Fronteiras e Logística** | *"Quais cidades tocam São Paulo e qual a distância delas?"* | `buscar_malha_municipal_vizinha` |
| **Polo Urbano Regional** | *"O que move a economia da Região Imediata de Campinas?"* | `buscar_perfil_economico_regiao_ibge` |
| **Top Setores por Cidade** | *"Quais são os 10 maiores setores de Joinville e qual a fatia de MEI?"* | `buscar_perfil_economico_municipio` |
| **Risco e Mortalidade** | *"Padarias têm boa taxa de sobrevivência em Goiânia?"* | `buscar_dinamica_sobrevivencia_cnae` |
| **Qualificação de Porte / B2B** | *"Quantas empresas de TI em Curitiba não são MEI?"* | `buscar_dinamica_sobrevivencia_cnae` |
| **Afinidade de Negócios** | *"Quem vende autopeças também faz mecânica?"* | `buscar_coocorrencia_cnaes_global` |
| **Diferenças Regionais** | *"O que os advogados de Salvador oferecem a mais?"* | `buscar_coocorrencia_cnaes_municipio` |
| **Expansão de Franquias** | *"Que tipo de filial uma matriz de logística abre?"* | `buscar_expansao_filiais_cnae` |
| **Visão Macrorregional** | *"Como o Nordeste está articulado no grafo?"* | `buscar_visao_macro_regiao` |

