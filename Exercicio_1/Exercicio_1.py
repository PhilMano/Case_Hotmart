# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
# Iniciar sessão Spark
spark = SparkSession.builder.appName("RenameColumn").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### # Utilizei o Databricks Free Edition para realziar as tarefas, por esse motivo sera necessario criar um volume nomeado de hotmart_case que será utilizado como repositório para armazenar os arquivos CSV que serão utilizados para as criaões das tabelas.

# COMMAND ----------

schema_purchase = StructType([
    StructField("purchase_id", LongType(), True),      
    StructField("buyer_id", LongType(), True),      
    StructField("prod_item_id", LongType(), True),
    StructField("order_date", DateType(), True),
    StructField("release_date", DateType(), True),
    StructField("producer_id", LongType(), True),
    StructField("purchase_partition", LongType(), True),
    StructField("prod_item_partition", LongType(), True),
    ])

schema_prod_item = StructType([
    StructField("prod_item_id", LongType(), True),      
    StructField("prod_item_partition", LongType(), True),      
    StructField("product_id", LongType(), True),
    StructField("item_quantity", LongType(), True),
    StructField("purchase_value", DoubleType(), True)
    ])


# COMMAND ----------

path_files = '/Volumes/workspace/default/hotmart_case'

# COMMAND ----------

purchase_csv = spark.read.format("csv").option("header", "true").schema(schema_purchase).load(f"{path_files}/purchase.csv")
product_item_csv = spark.read.format("csv").option("header", "true").schema(schema_prod_item).load(f"{path_files}/product_item.csv")


# COMMAND ----------

#purchase.show()
#product_item.show()
purchase_csv.write.format('parquet').mode('overwrite').partitionBy('prod_item_partition').save(f'{path_files}/purchase')
product_item_csv.write.format('parquet').mode('overwrite').partitionBy('prod_item_partition').save(f'{path_files}/product_item')
# DBTITLE 1')


# COMMAND ----------

# MAGIC %md
# MAGIC Leitura do arquivos no Formato Parquet

# COMMAND ----------


purchase = spark.read.parquet(f'{path_files}/purchase')
product_item = spark.read.parquet(f'{path_files}/product_item')


# COMMAND ----------

purchase.show()


# COMMAND ----------

product_item.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Criando as tabelas temporarias gold

# COMMAND ----------

purchase.createOrReplaceTempView("purchase_gold")
product_item.createOrReplaceTempView("product_item_gold")


# COMMAND ----------



# COMMAND ----------

# Durante o ciclo dos dados na arquitetura medalhão é verificado algumas conformidades de dados, entre elas a presenca de valores nulos nas tabelas. Algumas colunas pode ocorrer de ter valores nulos, entretanto outras não. Um exemplo de uma coluna da tabela purchase_gold que aceita valores nulos é a release_date, pois o pedido pode ter sido realizado (order_date) mas ainda não ter sido pago então faz sentido a release_date ser nulo. No entanto as outras colunas não aceitam tal ocorrencia, uma vez que são dados obrigatorios. 
spark.sql(
    """     select 
                SUM(CASE WHEN purchase_id IS NULL THEN 1 ELSE 0 END) AS total_null_purchase_id,
                SUM(CASE WHEN buyer_id IS NULL THEN 1 ELSE 0 END) AS total_null_purchase_buyer_id,
                SUM(CASE WHEN prod_item_id IS NULL THEN 1 ELSE 0 END) AS total_null_purchase_prod_item_id,
                SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS total_null_purchase_order_date,
                SUM(CASE WHEN release_date IS NULL THEN 1 ELSE 0 END) AS total_null_purchase_release_date,
                SUM(CASE WHEN producer_id IS NULL THEN 1 ELSE 0 END) AS tototal_null_producer_id,
                SUM(CASE WHEN purchase_partition IS NULL THEN 1 ELSE 0 END) AS total_null_purchase_partition,
                SUM(CASE WHEN purchase_partition IS NULL THEN 1 ELSE 0 END) AS total_null_purchase_partition
            from purchase_gold
            
          """
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Quais são os 50 maiores produtores em faturamento ($) de 2021?

# COMMAND ----------

spark.sql("""
          select p.producer_id,   
            round(sum(pi.purchase_value * pi.item_quantity), 2) as total_sale  
            from product_item_gold pi
          inner join purchase_gold p on pi.prod_item_id = p.prod_item_id and pi.prod_item_partition = p.prod_item_partition    
          where 1=1 
                and p.release_date is not null
                and p.order_date between '2021-01-01' and '2021-12-31'
          group by p.producer_id
          order by total_sale desc
          limit 50;
          """).show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ●​ Quais são os 2 produtos que mais faturaram ($) de cada produtor?

# COMMAND ----------

# MAGIC %md
# MAGIC Foi realizado o filtro no where para verificar qual release_date era nula, isso elimina a possibilidade de fazer o calculo de vendas totais em compras que nao foram efetuadas (a coluna release_date de acordo com diagrama representa a data de liberacao da compra mediante pagamento), porem algumas tabelas possuem mais uma coluna de status que indica se a prova foi cancelada, aprovada, pendente ou reembolsada. Essa coluna deve ser usada juntamente com release_date em filtros para selecioarnsomente as colunas com pagamento aprovado, é uma etapa a mais de verificacao.

# COMMAND ----------

spark.sql("""
                with product_billing as (
                    select 
                        pi.product_id as product_id,
                        p.producer_id,
                        round(sum(pi.purchase_value * pi.item_quantity), 2) as total_sales
                    from product_item pi 
                    join purchase p on pi.prod_item_id = p.prod_item_id and pi.prod_item_partition = p.prod_item_partition
                    where 1=1
                          and p.release_date is not null
                    group by pi.product_id, p.producer_id
                ),
                 product_sold_rank as (
                     select  product_id,
                            producer_id,
                            total_sales,
                            row_number() over (partition by producer_id order by total_sales desc) as rank_sales
                     from product_billing 
                 )

                select producer_id,
                        product_id,
                        total_sales,
                        rank_sales
                from product_sold_rank
                where rank_sales <=2
                order by producer_id;
    
          """).show()

# COMMAND ----------

