# Orion Varejo - Data Pipeline

Projeto de Engenharia de Dados desenvolvido para construção de um pipeline de dados da **Orion Varejo**, contemplando ingestão, processamento, armazenamento em camadas e disponibilização dos dados para análise.

A solução utiliza **SQL Server, Apache Spark, Docker, Amazon S3 e Apache Airflow**, seguindo uma arquitetura de dados organizada nas camadas **Bronze, Silver e Gold**.

Ao final do pipeline, os dados tratados serão disponibilizados para análise e construção de dashboards utilizando **Power BI**.

## Arquitetura do Projeto

SQL Server → Apache Spark → Amazon S3 → Bronze → Silver → Gold → Power BI

O **Apache Airflow** será utilizado para orquestrar e automatizar as diferentes etapas do pipeline.

## Tecnologias Utilizadas

- **SQL Server** — banco de dados legado utilizado como fonte dos dados.
- **Apache Spark / PySpark** — processamento, ingestão e transformação dos dados.
- **Docker** — criação do ambiente conteinerizado para execução do Apache Spark.
- **Amazon S3** — armazenamento das camadas de dados Bronze, Silver e Gold.
- **Apache Airflow** — orquestração e automação das etapas do pipeline.
- **Power BI** — análise dos dados e construção de dashboards.
- **Git e GitHub** — versionamento e gerenciamento do código-fonte.

## Estrutura do Projeto

```text
orion-varejo-data-pipeline/
├── scripts/
│   ├── ingestao_bronze.py
│   └── teste_conexao_sqlserver.py
├── docker-compose.yml
├── .gitignore
└── README.md