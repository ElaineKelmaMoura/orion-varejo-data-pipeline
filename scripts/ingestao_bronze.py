from pyspark.sql import SparkSession
import os



# 1. LEITURA DAS VARIÁVEIS DE AMBIENTE

sql_user = os.getenv("SQLSERVER_USER")
sql_password = os.getenv("SQLSERVER_PASSWORD")

aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")


# Validação para evitar executar o pipeline sem credenciais.
variaveis_obrigatorias = {
    "SQLSERVER_USER": sql_user,
    "SQLSERVER_PASSWORD": sql_password,
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



# 2. CRIAÇÃO DA SESSÃO SPARK

spark = (
    SparkSession.builder
    .appName("OrionVarejo_Ingestao_Bronze")

    # Drivers utilizados pelo Spark:
    # - SQL Server JDBC
    # - Hadoop AWS
    # - AWS SDK
    .config(
        "spark.jars",
        "/opt/spark/work-dir/jars/mssql-jdbc.jar,"
        "/opt/spark/work-dir/jars/hadoop-aws-3.3.4.jar,"
        "/opt/spark/work-dir/jars/aws-java-sdk-bundle-1.12.262.jar"
    )

    # Implementação do Hadoop responsável pelo protocolo s3a://
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    # Provedor de credenciais baseado em Access Key e Secret Key.
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )

    # Credenciais da AWS recebidas pelas variáveis de ambiente.
    .config(
        "spark.hadoop.fs.s3a.access.key",
        aws_access_key
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        aws_secret_key
    )

    # Endpoint do Amazon S3 na região utilizada pelo projeto.
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        f"s3.{aws_region}.amazonaws.com"
    )

    # Mantém o acesso usando o padrão regional do S3.
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "false"
    )

    .getOrCreate()
)


# Reduz a quantidade de mensagens técnicas exibidas no terminal.
spark.sparkContext.setLogLevel("WARN")



# 3. CONFIGURAÇÃO DA FONTE SQL SERVER

jdbc_url = (
    "jdbc:sqlserver://host.docker.internal:1433;"
    "databaseName=OrionVarejo_Legado;"
    "encrypt=true;"
    "trustServerCertificate=true;"
)


# Tabelas que serão extraídas do banco legado.
tabelas = [
    "Clientes",
    "Categorias",
    "Produtos",
    "Pedidos",
    "Itens_Pedido",
]



# 4. INGESTÃO DAS TABELAS PARA A CAMADA BRONZE NO S3

for tabela in tabelas:
    print(f"\nIniciando ingestão da tabela: {tabela}")

    df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"dbo.{tabela}")
        .option("user", sql_user)
        .option("password", sql_password)
        .option(
            "driver",
            "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        )
        .load()
    )

    quantidade = df.count()
    print(f"Registros lidos: {quantidade}")

    # Caminho da camada Bronze no Amazon S3.
    caminho_bronze = (
        f"s3a://orion-varejo-data-lake-ek/"
        f"bronze/{tabela.lower()}"
    )

    (
        df.write
        .mode("overwrite")
        .parquet(caminho_bronze)
    )

    print(
        f"Tabela {tabela} gravada com sucesso "
        f"em {caminho_bronze}"
    )


print("\nIngestão da camada Bronze concluída com sucesso.")

spark.stop()