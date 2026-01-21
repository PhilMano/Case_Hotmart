# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as  F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
# Iniciar sessão Spark
spark = SparkSession.builder.appName("RenameColumn").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC A tabela purchase tera compra valida somente quando o campo release_date IS NOT NULL, uma vez que a coluna possui a data de liberacao da compra mediante a confirmação do pagamento.
# MAGIC De acordo com a proposta do Case, o dado pode chegar atrasado ou ser reenviado, logo campos historicos podem mudar requerendo assim modelagem do tipo SCD2 (Slowly Changing Dimensions 2).  

# COMMAND ----------

# MAGIC %md
# MAGIC A tabela product_item possui a coluna purchase_value que indica o valor do item da compra, que será utilizado como parte da regra de negócio

# COMMAND ----------

# MAGIC %md
# MAGIC A coluna purchase_extra_info contém a subsidiária e pode haver casos em que os dados podem atualizar na tabela depois da purchase.

# COMMAND ----------

# MAGIC %md
# MAGIC As tabelas são assincronas decorrente do fato de serem CDC (Change Data Capture).
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC A seguir tem o fluxograma de execucao para a proposta do Case_Hotmart Exercicio_2

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Workspace/Users/philipemanoel70@gmail.com/Case_Hotmart/Exercicio_2/fluxograma.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Criando os DataFrames
# MAGIC Utilizando conceitos da Arquitetura Medalhao, os dataframes criados a seguir irao compor a camada bronze, pois nenhum tipo de tratamento, limpeza e aplicacao de regra de negocio foi realizado nessa etapa.

# COMMAND ----------

# Criando datframe purchase (cdc)

purchase_data = [
    ("2023-01-20 22:00:00", "2023-01-20", 55, 15947, 5, "2023-01-20", "2023-01-20", 852852),
    ("2023-01-26 00:01:00", "2023-01-26", 56, 369798, 746520, "2023-01-25", None, 963963),
    ("2023-02-05 10:00:00", "2023-02-05", 55, 160001, 5, "2023-01-20", "2023-01-20", 852852),
    ("2023-02-26 03:00:00", "2023-02-26", 69, 160001, 18, "2023-02-26", "2023-02-28", 96967),
    ("2023-07-15 09:00:00", "2023-07-15", 55, 160001, 5, "2023-01-20", "2023-03-01", 852852)
]

purchase_cols = [
    "transaction_datetime", "transaction_date", "purchase_id",
    "buyer_id", "prod_item_id", "order_date",
    "release_date", "producer_id"
]

purchase_df = (
    spark.createDataFrame(purchase_data, purchase_cols)
    .withColumn("transaction_datetime", F.to_timestamp("transaction_datetime"))
    .withColumn("transaction_date", F.to_date("transaction_date"))
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("release_date", F.to_date("release_date"))
)

purchase_df.show()

# COMMAND ----------

# Criando Dataframe product_item (cdc)

product_item_data = [
    ("2023-01-20 22:02:00", "2023-01-20", 55, 696969, 10, 50.00),
    ("2023-01-25 23:59:59", "2023-01-25", 56, 808080, 120, 2400.00),
    ("2023-02-26 03:00:00", "2023-02-26", 69, 373737, 2, 2000.00),
    ("2023-07-12 09:00:00", "2023-07-12", 55, 696969, 10, 55.00)
]

product_item_cols = [
    "transaction_datetime", "transaction_date", "purchase_id",
    "product_id", "item_quantity", "purchase_value"
]

product_item_df = (
    spark.createDataFrame(product_item_data, product_item_cols)
    .withColumn("transaction_datetime", F.to_timestamp("transaction_datetime"))
    .withColumn("transaction_date", F.to_date("transaction_date"))
)

product_item_df.show()


# COMMAND ----------

# Criando datraframe extra_item (cdc)
extra_info_data = [
    ("2023-01-23 00:05:00", "2023-01-23", 55, "nacional"),
    ("2023-01-25 23:59:59", "2023-01-25", 56, "internacional"),
    ("2023-02-28 01:10:00", "2023-02-28", 69, "nacional"),
    ("2023-03-12 07:00:00", "2023-03-12", 69, "internacional")
]

extra_info_cols = [
    "transaction_datetime", "transaction_date", "purchase_id", "subsidiary"
]

extra_info_df = (
    spark.createDataFrame(extra_info_data, extra_info_cols)
    .withColumn("transaction_datetime", F.to_timestamp("transaction_datetime"))
    .withColumn("transaction_date", F.to_date("transaction_date"))
)

extra_info_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Filtrando pelo ultimo estado do dado  'purchase_id' dos datyaframe purchase, product_item e extra_info.

# COMMAND ----------

# MAGIC %md
# MAGIC As etapas a seguir compoe a camada Silver da Arquitetura Medalhao

# COMMAND ----------

def norm_cdc(df, col_purchase):
    last_event = Window.partitionBy(col_purchase).orderBy(F.col("transaction_datetime").desc())
    return (
        df
        .withColumn("rn", F.row_number().over(last_event))
        .filter("rn = 1")
        .drop("rn")
    )


purchase = norm_cdc(purchase_df, 'purchase_id')
product_item = norm_cdc(product_item_df, 'purchase_id')
extra_info = norm_cdc(extra_info_df, 'purchase_id')        


# COMMAND ----------

# MAGIC %md
# MAGIC Nesse momento uma das regras de negócio é aplicaa, o GMV só tem validade quando confirmado pagamento, Filtrando compras com pagamento confirmado, garantindo desse modo a data unica e com pagamento confirmado (F.col("release_date").isNotNull()). Em alguns casos pode haver a coluna de status, quando essa estiver presente deve ser incluído ao filtro somente as ocorrencias em que o status = Aprovado bem como o release_date not null.

# COMMAND ----------


purchase_valid = (
    purchase
    .filter(F.to_date("release_date").isNotNull())
    .withColumn(
        "gmv_date",
        F.coalesce(
            F.col("release_date"),
            F.col("transaction_date"),
            F.col("order_date")
        )
    )
)


# COMMAND ----------

purchase_valid.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Criando fato com a compra consolidada (nao ssera a fato do sistema estrela de Data Warehouse), nesse momento a coluna gmv_date do df purchase_valid 'filtra' os dados para a granularidade ser diaria. <br/>
# MAGIC A juncao via outer se faz necessário, a tabela purchase é a principal e ja está validada. <br/>
# MAGIC Em cenários assincronos (como o do exemplo) se tentassemos fazer qualquer juncao que nao fosse outer ia ocorrer perda de dados. Usando outer  se existir o item ele é trazido para o join, caso não haja correspondecia, o join ainda sim mantem os dados de purchase.

# COMMAND ----------

fact_purchase = (
    purchase_valid.alias("pv")
    .join(product_item.alias("pil"), "purchase_id", "outer")
    .join(extra_info.alias("eil"), "purchase_id", "outer")
    .select(
    F.col("pv.gmv_date").alias("transaction_date"),
    F.col("eil.subsidiary"),
    F.col("pil.purchase_value").cast("double")
)
)

fact_purchase.show()

# COMMAND ----------

# MAGIC %md
# MAGIC GMV do dia para cada subsidiaria.
# MAGIC

# COMMAND ----------

gmv_daily_subs =  (fact_purchase
                .filter(F.col("release_date").isNotNull())
                .groupBy("transaction_date", "subsidiary")
                .agg(F.sum("purchase_value").alias("gmv_daily"))
)

gmv_daily_subs.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Icremento do dado de GMZ diario para dado historico.

# COMMAND ----------

gmv_scd = (
    gmv_daily_subs
    .withColumn("valid_from", F.current_date())
    .withColumn("valid_to", F.lit("9999-12-31").cast("date"))
    .withColumn("is_current", F.lit(True))
    .withColumn("load_timestamp", F.current_timestamp())
)


# COMMAND ----------

gmv_scd.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Criacao da tabela gold.<br/> 
# MAGIC Verificado se a tabela fact_gmv_daily_scd2 existe, caso contratio a tabela sera criada. <br/> 
# MAGIC A partir do momento da criacao os dados e da primeira insercao os dados subsequentes serao comparados com os presentes na tabela a partir da chave gmv_daily, em caso de existencia o valor do campo valid_to e is_current serao alterados (agora virou dado historico) e o dado mais atual sera inserido.
# MAGIC  
# MAGIC Durante o momento da criacao a tabela sera particionada por transaction_date

# COMMAND ----------

# MAGIC %md
# MAGIC Na etapa Silver algumas ferramentas de controle de qualidade de dados podem ser aplicadas para verificar conformidade dos fatos antes da insercao na camada Gold, tem-se como exemplo a Great Expectation (GE).

# COMMAND ----------

target_table = "analytics.fact_gmv_daily_scd2"

spark.sql("CREATE DATABASE IF NOT EXISTS analytics")

if not spark.catalog.tableExists(target_table):
    (
        gmv_scd
        .write
        .format("delta")
        .partitionBy("transaction_date")
        .saveAsTable(target_table)
    )
else:
    delta_table = DeltaTable.forName(spark, target_table)

    (
        delta_table.alias("t_hist")
        .merge(
            gmv_scd.alias("s_currently"),
            """
            t_hist.transaction_date = s_currently.transaction_date
            AND t_hist.subsidiary = s_currently.subsidiary
            AND t_hist.is_current = true
            """
        )
        .whenMatchedUpdate(
            condition="t_hist.gmv_daily <> s_currently.gmv_daily",
            set={
                "valid_to": F.current_date() - F.expr("INTERVAL 1 DAY"),
                "is_current": "false"
            }
        )
        .whenNotMatchedInsert(values={
            "transaction_date": "s_currently.transaction_date",
            "subsidiary": "s_currently.subsidiary",
            "gmv_daily": "s_currently.gmv_daily",
            "valid_from": "s_currently.valid_from",
            "valid_to": "s_currently.valid_to",
            "is_current": "s_currently.is_current",
            "load_timestamp": "s_currently.load_timestamp"
        })
        .execute()
    )


# COMMAND ----------

# MAGIC %md
# MAGIC A camada Gold sera composta por analytics.fact_gmv_daily_scd2, as regras de negocios estao aplicadas e podem ser realizadas as consultas desejadas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     transaction_date,
# MAGIC     subsidiary,
# MAGIC     gmv_daily
# MAGIC FROM analytics.fact_gmv_daily_scd2
# MAGIC WHERE is_current = true
# MAGIC and transaction_date IS NOT NULL
# MAGIC ORDER BY transaction_date, subsidiary;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM analytics.fact_gmv_daily_scd2
# MAGIC WHERE is_current = true
# MAGIC and transaction_date IS NOT NULL
# MAGIC ORDER BY transaction_date, subsidiary;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     transaction_date,
# MAGIC     subsidiary,
# MAGIC     SUM(gmv_daily) AS gmv
# MAGIC FROM analytics.fact_gmv_daily_scd2
# MAGIC WHERE transaction_date BETWEEN '2023-01-01' AND '2023-03-31'
# MAGIC GROUP BY transaction_date, subsidiary;

# COMMAND ----------

# MAGIC %md
# MAGIC DDLS da Tabela fact_gmv_daily_scd2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS analytics.fact_gmv_daily_scd2 (
# MAGIC     transaction_date DATE COMMENT 'Data de referência do GMV (data de liberação do pagamento)',
# MAGIC     subsidiary STRING COMMENT 'Subsidiária associada à transação',
# MAGIC     gmv_daily DOUBLE COMMENT 'Gross Merchandise Value diário por subsidiária',
# MAGIC     valid_from DATE COMMENT 'Data de início de vigência do registro (SCD2)',
# MAGIC     valid_to DATE COMMENT 'Data de fim de vigência do registro (SCD2)',
# MAGIC     is_current BOOLEAN COMMENT 'Indicador de registro vigente',
# MAGIC     load_timestamp TIMESTAMP COMMENT 'Timestamp de carga do registro no lake'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (transaction_date)
# MAGIC COMMENT 'Tabela histórica SCD2 com GMV diário por subsidiária, permitindo reprocessamentos e correções tardias sem alteração do passado';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Exemplode de Arquitetura de Pipeline a qual os scripts anteriores podem ser incoporadas, claro, com as devidas adequações.

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Workspace/Users/philipemanoel70@gmail.com/Case_Hotmart/Exercicio_2/etl_arq_aws)