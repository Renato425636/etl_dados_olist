# Framework de Pipeline ETL Dimensional com PySpark

Este repositório apresenta um framework de ETL (Extração, Transformação e Carregamento) avançado, construído em Python e PySpark. O projeto foi refatorado para uma arquitetura orientada a objetos, tornando-o uma base reutilizável e configurável para transformar dados transacionais brutos em um modelo dimensional (Star Schema) otimizado para consultas analíticas (OLAP).

## 🎯 Objetivo do Projeto

O objetivo é fornecer um exemplo completo e profissional de um pipeline de dados moderno, demonstrando soluções para desafios comuns e implementando boas práticas de engenharia de dados, como:

  * **Reusabilidade e Manutenibilidade:** Criar um código encapsulado em uma classe que seja fácil de manter, estender e adaptar para diferentes necessidades.
  * **Governança de Dados:** Implementar um framework de verificação de qualidade de dados (Data Quality) e gerar perfis de dados (Data Profiling) para garantir a confiabilidade e o monitoramento contínuo dos dados.
  * **Configuração Desacoplada:** Separar a lógica do pipeline de suas configurações de ambiente, permitindo que ele seja executado em diferentes cenários (desenvolvimento, produção) sem alteração de código.
  * **Modelagem Dimensional Extensível:** Construir um Star Schema e demonstrar como ele pode ser facilmente expandido com novas dimensões e fatos.

O projeto utiliza o dataset público da **[Olist E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)** para simular um ambiente de dados transacionais real.

## ✨ Principais Funcionalidades

Este framework vai além de um simples script de ETL, incorporando funcionalidades de nível de produção:

  * **Arquitetura Orientada a Objetos:** Toda a lógica do pipeline é encapsulada na classe `AdvancedStarSchemaPipeline`, promovendo organização, reusabilidade e testabilidade.
  * **Configuração via YAML:** Todas as configurações, como caminhos, nomes e parâmetros, são gerenciadas em um arquivo `config.yaml` externo, permitindo fácil alteração sem tocar no código-fonte.
  * **Framework de Qualidade de Dados (DQ):**
      * **Verificação de Chaves:** Garante a unicidade e a não nulidade das chaves primárias nas dimensões.
      * **Integridade Referencial:** Valida se todas as chaves estrangeiras na tabela de fatos existem em suas respectivas dimensões (ex: `left_anti` join).
      * **Valores Aceitos:** Confere se colunas categóricas (como `order_status`) contêm apenas valores de uma lista predefinida.
      * **Validação de Intervalo:** Assegura que colunas numéricas (como `preco`) não contenham valores inválidos (ex: negativos).
  * **Perfil de Dados Automatizado (Data Profiling):** Gera um relatório em JSON com estatísticas descritivas (média, desvio padrão, min, max, etc.) para colunas críticas, auxiliando na detecção de anomalias e data drift.
  * **Modelo Dimensional Extensível:** O design permite adicionar novas dimensões facilmente. O framework já inclui a **`Dim_Geolocalizacao`** como prova de conceito.
  * **Schemas Explícitos e Forçados:** Define e aplica schemas rigorosos na leitura dos dados de origem, prevenindo erros de tipo e garantindo a estabilidade do pipeline.

## ⚙️ Tecnologias Utilizadas

  * **Python:** Linguagem principal para orquestração.
  * **Apache Spark (PySpark):** Motor de processamento de dados distribuído.
  * **PyYAML:** Para carregar e gerenciar o arquivo de configuração `config.yaml`.
  * **Requests:** Para download dos dados.

## 🗂️ Estrutura de Arquivos do Projeto

O pipeline assume e cria a seguinte estrutura de diretórios para organizar os artefatos de dados:

```
.
├── config.yaml                 # Arquivo de configuração principal
├── pipeline.py                 # O script do framework ETL
└── data/
    ├── source/                 # Dados brutos baixados da Olist (.csv)
    ├── dimensional/            # Modelos dimensionais finais (.parquet)
    │   ├── Dim_Cliente/
    │   ├── Dim_Produto/
    │   ├── Dim_Vendedor/
    │   ├── Dim_Tempo/
    │   ├── Dim_Geolocalizacao/
    │   └── Fato_Vendas/
    └── profiling/
        └── Fato_Vendas_profile.json # Relatório de perfil de dados
```

## 🚀 Como Executar o Pipeline

Siga os passos abaixo para configurar e rodar o projeto.

### 1\. Pré-requisitos

  * Python 3.9 ou superior.
  * Java Development Kit (JDK) 8 ou 11.
  * Clone o repositório.
  * Instale as dependências Python:
    ```bash
    pip install pyspark requests pyyaml
    ```

### 2\. Crie o Arquivo de Configuração

No diretório raiz do projeto, crie o arquivo **`config.yaml`** com o conteúdo abaixo:

```yaml
# Configurações Gerais do Pipeline
pipeline_name: "AdvancedStarSchemaPipeline"
log_level: "INFO"

# Configurações do Spark
spark:
  master: "local[*]"
  app_name: "AdvancedStarSchemaPipeline"

# Configurações de Dados
data:
  url: "https://github.com/g-bolota/olist-dataset/raw/main/olist_ecommerce_dataset.zip"
  source_path: "data/source"
  output_path: "data/dimensional"
  profiling_path: "data/profiling"

# Configurações de Qualidade de Dados
data_quality:
  accepted_order_status:
    - "delivered"
    - "shipped"
    - "canceled"
    - "invoiced"
    - "processing"
    - "unavailable"
    - "approved"
    - "created"
```

### 3\. Execute o Pipeline

Com o arquivo `config.yaml` no lugar, execute o script principal:

```bash
python pipeline.py
```

O script irá automaticamente baixar os dados (na primeira execução), processá-los, executar as validações, gerar o perfil e salvar os modelos dimensionais.

## 🌠 O Modelo Dimensional (Star Schema)

O pipeline constrói o seguinte esquema estrela, otimizado para consultas analíticas:

  * **Tabela Fato:**
      * `Fato_Vendas`: Contém as métricas de negócio (`preco`, `valor_frete`) e as chaves para as dimensões.
  * **Tabelas de Dimensão:**
      * `Dim_Cliente`: Descreve os clientes (cidade, estado, geolocalização).
      * `Dim_Produto`: Descreve os produtos (categoria, quantidade de fotos).
      * `Dim_Vendedor`: Descreve os vendedores (cidade, estado, geolocalização).
      * `Dim_Tempo`: Descreve o tempo (ano, mês, dia, trimestre).
      * `Dim_Geolocalizacao`: Descreve a localização geográfica (latitude, longitude) a partir do CEP.

## 📈 Melhorias Futuras

Este framework é uma base sólida que pode ser ainda mais aprimorada com:

  * **Orquestração de Workflows:** Integração com ferramentas como **Apache Airflow**, **Prefect** ou **Mage** para agendar e monitorar execuções de forma robusta.
  * **Containerização:** Empacotar a aplicação com **Docker** para garantir a portabilidade e consistência entre diferentes ambientes.
  * **Testes Automatizados:** Implementação de testes unitários e de integração com `pytest` para validar a lógica de transformação de forma isolada.
  * **Cargas Incrementais e SCD:** Adicionar lógica para lidar com cargas incrementais (processando apenas dados novos) e Slowly Changing Dimensions (SCD Tipo 2).

-----
