from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
import os


# LEITURA DAS VARIÁVEIS DE AMBIENTE

aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

variaveis_obrigatorias = {
    "AWS_ACCESS_KEY_ID": aws_access_key,
    "AWS_SECRET_ACCESS_KEY": aws_secret_key,
}

variaveis_ausentes = [
    nome
    for nome, valor in variaveis_obrigatorias.items()
    if not valor
]

if variaveis_ausentes:
    raise ValueError(
        "Variáveis de ambiente ausentes: "
        + ", ".join(variaveis_ausentes)
    )

# CRIAÇÃO DA SESSÃO SPARK

spark = (
    SparkSession.builder
    .appName("OrionVarejo_Transformacao_Silver")

    .config(
        "spark.jars",
        "/opt/spark/work-dir/jars/mssql-jdbc.jar,"
        "/opt/spark/work-dir/jars/hadoop-aws-3.3.4.jar,"
        "/opt/spark/work-dir/jars/aws-java-sdk-bundle-1.12.262.jar"
    )

    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )

    .config(
        "spark.hadoop.fs.s3a.access.key",
        aws_access_key
    )

    .config(
        "spark.hadoop.fs.s3a.secret.key",
        aws_secret_key
    )

    .config(
        "spark.hadoop.fs.s3a.endpoint",
        f"s3.{aws_region}.amazonaws.com"
    )

    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "false"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Tabelas que serão processadas na camada Silver.
tabelas = [
    "Clientes",
    "Categorias",
    "Produtos",
    "Pedidos",
    "Itens_Pedido"
]


# FUNÇÕES DE TRANSFORMAÇÃO

def ler_tabela_bronze(nome_tabela):
    """
    Lê uma tabela em formato Parquet na camada Bronze do S3.
    """

    caminho_bronze = (
        f"s3a://orion-varejo-data-lake-ek/"
        f"bronze/{nome_tabela.lower()}"
    )

    print(
        f"\nLendo a tabela {nome_tabela} "
        f"da camada Bronze."
    )

    dataframe = spark.read.parquet(caminho_bronze)

    quantidade = dataframe.count()

    print(
        f"Registros encontrados em {nome_tabela}: "
        f"{quantidade}"
    )

    return dataframe


def gravar_tabela_silver(dataframe, nome_tabela):
    """
    Grava uma tabela tratada em formato Parquet
    na camada Silver do S3.
    """

    caminho_silver = (
        f"s3a://orion-varejo-data-lake-ek/"
        f"silver/{nome_tabela.lower()}"
    )

    (
        dataframe.write
        .mode("overwrite")
        .parquet(caminho_silver)
    )

    print(
        f"Tabela {nome_tabela} gravada com sucesso em "
        f"{caminho_silver}"
    )





def transformar_clientes():
    """
    Aplica regras de qualidade e padronização
    à tabela Clientes.
    """

    print("\nIniciando transformação da tabela Clientes.")

    df_clientes = ler_tabela_bronze("Clientes")

    df_clientes_silver = (
        df_clientes
        .dropDuplicates(["id_cliente"])
        .filter(col("nome").isNotNull())
        .withColumn("email", lower(col("email")))
    )

    quantidade_silver = df_clientes_silver.count()

    print(
        f"Registros após tratamento em Clientes: "
        f"{quantidade_silver}"
    )

    gravar_tabela_silver(
        df_clientes_silver,
        "Clientes"
    )


# EXECUÇÃO PRINCIPAL

try:
    transformar_clientes()

    print(
        "\nTransformação da camada Silver "
        "concluída com sucesso."
    )

except Exception as erro:
    print(
        f"\nErro durante a transformação da camada Silver: {erro}"
    )
    raise

finally:
    spark.stop()