from pyspark.sql import SparkSession
import os

spark = (
    SparkSession.builder
    .appName("TesteConexaoSQLServer")
    .getOrCreate()
)

jdbc_url = (
    "jdbc:sqlserver://host.docker.internal:1433;"
    "databaseName=OrionVarejo_Legado;"
    "encrypt=true;"
    "trustServerCertificate=true;"
)

sql_user = os.getenv("SQLSERVER_USER")
sql_password = os.getenv("SQLSERVER_PASSWORD")

df_clientes = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.Clientes")
    .option("user", sql_user)
    .option("password", sql_password)
    .option(
        "driver",
        "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    )
    .load()
)

df_clientes.show(truncate=False)

spark.stop()
