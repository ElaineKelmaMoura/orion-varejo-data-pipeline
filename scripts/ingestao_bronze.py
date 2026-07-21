from pyspark.sql import SparkSession
import os

# Cria a sessão principal do Spark.
spark = (
    SparkSession.builder
    .appName("OrionVarejo_Ingestao_Bronze")
    .getOrCreate()
)

# Reduz a quantidade de mensagens técnicas no terminal.
spark.sparkContext.setLogLevel("WARN")

# Endereço JDBC do banco legado.
jdbc_url = (
    "jdbc:sqlserver://host.docker.internal:1433;"
    "databaseName=OrionVarejo_Legado;"
    "encrypt=true;"
    "trustServerCertificate=true;"
)

# Tabelas que serão extraídas do SQL Server.
tabelas = [
    "Clientes",
    "Categorias",
    "Produtos",
    "Pedidos",
    "Itens_Pedido"
]

sql_user = os.getenv("SQLSERVER_USER")
sql_password = os.getenv("SQLSERVER_PASSWORD")

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

    # Mostra quantos registros foram encontrados.
    quantidade = df.count()
    print(f"Registros lidos: {quantidade}")

    # Grava os dados brutos na camada Bronze em formato Parquet.
    caminho_bronze = f"/opt/spark/work-dir/data/bronze/{tabela.lower()}"

    (
        df.write
        .mode("overwrite")
        .parquet(caminho_bronze)
    )

    print(f"Tabela {tabela} gravada com sucesso na camada Bronze.")

print("\nIngestão da camada Bronze concluída com sucesso.")

spark.stop()
