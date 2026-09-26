# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Mini Lakehouse — Indian E-Commerce Sales Analytics
# MAGIC
# MAGIC **Objetivo:** construir um mini Lakehouse governado no Databricks, usando Unity Catalog, Delta Lake e arquitetura Medalhão.
# MAGIC
# MAGIC - `rower_bootcamp.landing.files` para os arquivos CSV;
# MAGIC - Auto Loader para ingestão incremental e idempotente da Bronze;
# MAGIC - `CREATE OR REPLACE TABLE` para reprocessar Silver e Gold a partir do estado atual da Bronze;
# MAGIC - namespace completo em todos os objetos persistentes.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Título e objetivo do projeto
# MAGIC
# MAGIC O projeto transforma os arquivos `sales*.csv` e `products*.csv` em dados confiáveis para análise de vendas. A solução mantém rastreabilidade da origem, aplica regras de qualidade, disponibiliza dois produtos analíticos Gold e restringe o consumo por meio de uma view governada.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Descrição da arquitetura Medalhão
# MAGIC
# MAGIC - **Landing:** guarda os arquivos originais em um Volume do Unity Catalog.
# MAGIC - **Bronze:** ingere os CSVs sem alterar os campos de negócio; acrescenta metadados do arquivo e da carga.
# MAGIC - **Silver:** tipa, normaliza, deduplica, separa inválidos, preserva pedidos sem cadastro de produto e cria campos derivados.
# MAGIC - **Gold:** entrega indicadores mensais e ranking de marcas.
# MAGIC - **View:** expõe somente as dez marcas de maior receita reconhecida.

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## 3. Criação ou identificação do catálogo
# MAGIC
# MAGIC A arquitetura está organizada nos seguintes schemas:
# MAGIC
# MAGIC - `rower_bootcamp.landing`: arquivos e estado técnico da ingestão;
# MAGIC - `rower_bootcamp.bronze`: dados brutos e rastreáveis;
# MAGIC - `rower_bootcamp.silver`: dados tratados e validados;
# MAGIC - `rower_bootcamp.gold`: tabelas analíticas e view de consumo.

# COMMAND ----------

# DBTITLE 1,Configurar o catálogo Lakehouse e a hierarquia de esquemas em camadas
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS rower_bootcamp
# MAGIC COMMENT 'Catálogo do mini Lakehouse Indian E-Commerce Sales Analytics';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS rower_bootcamp.landing
# MAGIC COMMENT 'Landing: arquivos originais e estado técnico de ingestão';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS rower_bootcamp.bronze
# MAGIC COMMENT 'Bronze: dados brutos, incrementais e rastreáveis';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS rower_bootcamp.silver
# MAGIC COMMENT 'Silver: dados validados, padronizados e enriquecidos';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS rower_bootcamp.gold
# MAGIC COMMENT 'Gold: produtos analíticos para consumo de negócio';

# COMMAND ----------

# DBTITLE 1,Examinar a estrutura do catálogo rower_bootcamp
# MAGIC %sql
# MAGIC DESCRIBE CATALOG rower_bootcamp;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Criação da Landing e do volume

# COMMAND ----------

# DBTITLE 1,Criar volume para arquivos do dataset e pontos de contr ...
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS rower_bootcamp.landing.files
# MAGIC COMMENT 'Arquivos do dataset e checkpoints de ingestão do projeto';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Evidência dos arquivos

# COMMAND ----------

# DBTITLE 1,Listar arquivos no diretório de landing do dataset
landing_path = "/Volumes/rower_bootcamp/landing/files/desafio"
arquivos = dbutils.fs.ls(landing_path)
print(f"Arquivos encontrados na landing: {len(arquivos)}")

# COMMAND ----------

# DBTITLE 1,Verificar presença de arquivos obrigatórios sales e pro ...
nomes = {arquivo.name.lower() for arquivo in arquivos}
assert any(nome.startswith("sales") and nome.endswith(".csv") for nome in nomes), \
    "Nenhum arquivo sales*.csv foi encontrado."
assert any(nome.startswith("products") and nome.endswith(".csv") for nome in nomes), \
    "Nenhum arquivo products*.csv foi encontrado."
print("Arquivos obrigatórios encontrados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Criação da Bronze — carga incremental com Auto Loader
# MAGIC
# MAGIC A Bronze usa nomes novos (`orders_raw` e `products_raw`) para não misturar a carga corrigida com as tabelas antigas do notebook original. Todos os campos do CSV entram como `STRING`; a tipagem pertence à Silver.
# MAGIC
# MAGIC O checkpoint registra quais arquivos já foram processados. Assim, uma segunda execução com os mesmos arquivos não duplica linhas.

# COMMAND ----------

# DBTITLE 1,Ingestão incremental de dados de vendas e produtos CSV
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

source_root = "/Volumes/rower_bootcamp/landing/files/desafio/"
state_root = "/Volumes/rower_bootcamp/landing/files/_system/autoloader"
dbutils.fs.mkdirs(state_root)

order_columns = [
    "Order_ID", "Customer_ID", "Product_ID", "Order_Date", "Order_Time",
    "Delivery_Date", "Quantity", "Unit_Price", "Order_Value",
    "Shipping_Cost", "Coupon_Code", "Coupon_Discount", "Total_Amount",
    "Payment_Mode", "Order_Status", "Rating", "Review_Text", "City",
    "State", "Customer_Age", "Customer_Age_Group"
]

product_columns = [
    "Product_ID", "Product_Name", "Category", "Brand", "Original_Price",
    "Discount_Percent", "Discount_Amount", "Selling_Price",
    "Stock_Quantity", "Weight_kg", "Avg_Rating", "Total_Reviews"
]

def string_schema(columns):
    return StructType([StructField(column, StringType(), True) for column in columns])

def ingest_csv_incrementally(file_glob, schema, target_table, state_name):
    stream_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{state_root}/{state_name}/schema")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .option("delimiter", ",")
        .option("pathGlobFilter", file_glob)
        .schema(schema)
        .load(source_root)
        .select(
            "*",
            F.current_timestamp().alias("ingestion_timestamp"),
            F.current_date().alias("ingestion_date"),
            F.col("_metadata.file_name").alias("source_file_name"),
            F.col("_metadata.file_path").alias("source_file_path"),
            F.col("_metadata.file_size").alias("source_file_size"),
            F.col("_metadata.file_modification_time").alias("source_file_modification_time")
        )
    )

    query = (
        stream_df.writeStream
        .format("delta")
        .option("checkpointLocation", f"{state_root}/{state_name}/checkpoint")
        .outputMode("append")
        .trigger(availableNow=True)
        .queryName(f"ingest_{state_name}")
        .toTable(target_table)
    )
    query.awaitTermination()

ingest_csv_incrementally(
    "sales*.csv",
    string_schema(order_columns),
    "rower_bootcamp.bronze.orders_raw",
    "orders"
)

ingest_csv_incrementally(
    "products*.csv",
    string_schema(product_columns),
    "rower_bootcamp.bronze.products_raw",
    "products"
)

# COMMAND ----------

# DBTITLE 1,Adicionar comentários às tabelas de pedidos e produtos
# MAGIC %sql
# MAGIC COMMENT ON TABLE rower_bootcamp.bronze.orders_raw IS
# MAGIC 'Pedidos brutos incrementais, preservados como texto e rastreáveis até o arquivo de origem';
# MAGIC
# MAGIC COMMENT ON TABLE rower_bootcamp.bronze.products_raw IS
# MAGIC 'Produtos brutos incrementais, preservados como texto e rastreáveis até o arquivo de origem';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validações da Bronze
# MAGIC
# MAGIC A validação deve confirmar quantidade de linhas, arquivos processados, metadados preenchidos e ausência de dados resgatados inesperadamente.

# COMMAND ----------

# DBTITLE 1,Contar Linhas e Arquivos em Tabelas de Pedidos e Produt ...
# MAGIC %sql
# MAGIC SELECT
# MAGIC     'orders_raw' AS object_name,
# MAGIC     COUNT(*) AS row_count,
# MAGIC     COUNT(DISTINCT source_file_path) AS source_files,
# MAGIC     SUM(CASE WHEN source_file_path IS NULL THEN 1 ELSE 0 END) AS missing_lineage,
# MAGIC     SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END) AS rescued_rows
# MAGIC FROM rower_bootcamp.bronze.orders_raw
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'products_raw',
# MAGIC     COUNT(*),
# MAGIC     COUNT(DISTINCT source_file_path),
# MAGIC     SUM(CASE WHEN source_file_path IS NULL THEN 1 ELSE 0 END),
# MAGIC     SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END)
# MAGIC FROM rower_bootcamp.bronze.products_raw;

# COMMAND ----------

# DBTITLE 1,Analisar arquivos de origem e ingestão de dados em lote
# MAGIC %sql
# MAGIC SELECT
# MAGIC     source_file_name,
# MAGIC     source_file_size,
# MAGIC     source_file_modification_time,
# MAGIC     MIN(ingestion_timestamp) AS first_ingestion_at,
# MAGIC     COUNT(*) AS ingested_rows
# MAGIC FROM rower_bootcamp.bronze.orders_raw
# MAGIC GROUP BY ALL
# MAGIC ORDER BY source_file_modification_time;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Teste de idempotência da carga Bronze
# MAGIC
# MAGIC Este teste registra as quantidades atuais das tabelas Bronze, executa novamente o Auto Loader utilizando os mesmos checkpoints e compara as quantidades antes e depois.
# MAGIC
# MAGIC Como os arquivos já foram registrados nos checkpoints, a segunda execução não deve inserir novamente os mesmos registros. O resultado esperado é `diferenca = 0` e `status = APROVADO` para as duas tabelas.

# COMMAND ----------

# DBTITLE 1,Registrar e validar a idempotência das tabelas Bronze

# ---------------------------------------------------------
# 1. Registra a quantidade atual das tabelas Bronze
# ---------------------------------------------------------

tabelas_bronze = {
    "orders_raw": "rower_bootcamp.bronze.orders_raw",
    "products_raw": "rower_bootcamp.bronze.products_raw"
}

contagem_antes = {
    nome: spark.table(tabela).count()
    for nome, tabela in tabelas_bronze.items()
}

print("Contagens antes da segunda execução:")
print(contagem_antes)


# ---------------------------------------------------------
# 2. Executa novamente o Auto Loader
# ---------------------------------------------------------
# São utilizados os mesmos:
# - arquivos de origem;
# - caminhos de checkpoint;
# - nomes das tabelas de destino.

ingest_csv_incrementally(
    "sales*.csv",
    string_schema(order_columns),
    "rower_bootcamp.bronze.orders_raw",
    "orders"
)

ingest_csv_incrementally(
    "products*.csv",
    string_schema(product_columns),
    "rower_bootcamp.bronze.products_raw",
    "products"
)


# ---------------------------------------------------------
# 3. Registra as quantidades depois da nova execução
# ---------------------------------------------------------

contagem_depois = {
    nome: spark.table(tabela).count()
    for nome, tabela in tabelas_bronze.items()
}


# ---------------------------------------------------------
# 4. Monta a evidência da idempotência
# ---------------------------------------------------------

resultado_idempotencia = []

for nome in tabelas_bronze:
    antes = contagem_antes[nome]
    depois = contagem_depois[nome]
    diferenca = depois - antes

    resultado_idempotencia.append(
        (
            nome,
            antes,
            depois,
            diferenca,
            "APROVADO" if diferenca == 0 else "REPROVADO"
        )
    )

df_idempotencia = spark.createDataFrame(
    resultado_idempotencia,
    [
        "tabela",
        "linhas_antes",
        "linhas_depois",
        "diferenca",
        "status"
    ]
)

display(df_idempotencia)


# ---------------------------------------------------------
# 5. Interrompe o notebook se houver duplicação
# ---------------------------------------------------------

tabelas_com_duplicacao = [
    linha["tabela"]
    for linha in df_idempotencia.collect()
    if linha["diferenca"] != 0
]

assert not tabelas_com_duplicacao, (
    "Falha no teste de idempotência. "
    f"As seguintes tabelas tiveram alteração inesperada: "
    f"{tabelas_com_duplicacao}"
)

print("APROVADO: a segunda execução não duplicou os dados da Bronze.")

# COMMAND ----------

# DBTITLE 1,Configurar ambiente de teste incremental para orders_ra ...
import csv
import io

# Caminhos exclusivos para o teste.
test_source_root = (
    "/Volumes/rower_bootcamp/landing/files/"
    "_tests/incremental_orders"
)

test_state_root = (
    "/Volumes/rower_bootcamp/landing/files/"
    "_system/tests/incremental_orders"
)

test_table = (
    "rower_bootcamp.bronze."
    "orders_raw_incremental_test"
)

# Remove somente os objetos anteriores do teste.
spark.sql(
    f"DROP TABLE IF EXISTS {test_table}"
)

dbutils.fs.rm(test_source_root, True)
dbutils.fs.rm(test_state_root, True)
dbutils.fs.mkdirs(test_source_root)

# Obtém dois produtos existentes.
product_ids = [
    row["Product_ID"]
    for row in (
        spark.table(
            "rower_bootcamp.bronze.products_raw"
        )
        .select("Product_ID")
        .where("Product_ID IS NOT NULL")
        .distinct()
        .limit(2)
        .collect()
    )
]

assert len(product_ids) == 2, (
    "Não foram encontrados dois produtos para o teste."
)

# Função para criar um arquivo CSV com uma linha.
def write_test_csv(file_name, row_values):
    buffer = io.StringIO()
    writer = csv.writer(buffer)

    writer.writerow(order_columns)
    writer.writerow(row_values)

    dbutils.fs.put(
        f"{test_source_root}/{file_name}",
        buffer.getvalue(),
        overwrite=False
    )

# COMMAND ----------

# DBTITLE 1,Gerar arquivo CSV com dados de vendas de teste incremen ...
order_columns = [
    "Order_ID", "Customer_ID", "Product_ID", "Order_Date", "Order_Time",
    "Delivery_Date", "Quantity", "Unit_Price", "Order_Value",
    "Shipping_Cost", "Coupon_Code", "Coupon_Discount", "Total_Amount",
    "Payment_Mode", "Order_Status", "Rating", "Review_Text", "City",
    "State", "Customer_Age", "Customer_Age_Group"
]

first_test_row = [
    "TEST_INCREMENTAL_001",
    "CUSTOMER_TEST_001",
    product_ids[0],
    "2026-09-22",
    "10:00:00",
    "2026-09-25",
    "1",
    "100.00",
    "100.00",
    "10.00",
    "",
    "0.00",
    "110.00",
    "UPI",
    "DELIVERED",
    "5",
    "Teste incremental lote 1",
    "Delhi",
    "DELHI",
    "30",
    "26-35"
]

write_test_csv(
    "sales_batch_01.csv",
    first_test_row
)

# COMMAND ----------

# DBTITLE 1,Ingestão de dados incrementais de vendas para Delta Lak ...
def ingest_incremental_test():
    test_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option(
            "cloudFiles.schemaLocation",
            f"{test_state_root}/schema"
        )
        .option(
            "cloudFiles.includeExistingFiles",
            "true"
        )
        .option("header", "true")
        .option("delimiter", ",")
        .option("pathGlobFilter", "sales*.csv")
        .schema(string_schema(order_columns))
        .load(test_source_root)
    )

    query = (
        test_stream.writeStream
        .format("delta")
        .option(
            "checkpointLocation",
            f"{test_state_root}/checkpoint"
        )
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(test_table)
    )

    query.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Verificar contagem de registros no lote incremental de  ...
from pyspark.sql.types import StructType, StructField, StringType

def string_schema(columns):
    return StructType([StructField(column, StringType(), True) for column in columns])

ingest_incremental_test()

first_count = spark.table(test_table).count()

assert first_count == 1, (
    f"Esperado 1 registro, encontrado: {first_count}"
)

print(f"Primeiro lote aprovado: {first_count} registro.")

# COMMAND ----------

# DBTITLE 1,Validar carga incremental com novo arquivo de vendas
second_test_row = [
    "TEST_INCREMENTAL_002",
    "CUSTOMER_TEST_002",
    product_ids[1],
    "2026-09-23",
    "11:00:00",
    "",
    "1",
    "200.00",
    "200.00",
    "20.00",
    "TESTE10",
    "10.00",
    "210.00",
    "CREDIT CARD",
    "SHIPPED",
    "4",
    "Teste incremental lote 2",
    "Mumbai",
    "MAHARASHTRA",
    "40",
    "36-45"
]

write_test_csv(
    "sales_batch_02.csv",
    second_test_row
)

ingest_incremental_test()

second_count = spark.table(test_table).count()

assert second_count == 2, (
    f"Esperado 2 registros, encontrado: {second_count}"
)

print(
    "Carga incremental aprovada: "
    "somente o novo arquivo foi adicionado."
)

# COMMAND ----------

# DBTITLE 1,Testar Carga Incremental e Idempotência de Registros
ingest_incremental_test()

third_count = spark.table(test_table).count()

assert third_count == second_count, (
    "A reexecução duplicou registros."
)

resultado_incremental = [
    ("PRIMEIRO_LOTE", first_count),
    ("SEGUNDO_LOTE", second_count),
    ("REEXECUCAO", third_count)
]

df_incremental_test = spark.createDataFrame(
    resultado_incremental,
    ["etapa", "total_registros"]
)

display(df_incremental_test)

print(
    "APROVADO: carga incremental e "
    "idempotência comprovadas."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Criação da Silver
# MAGIC
# MAGIC Primeiro são criadas duas views temporárias: pedidos tipados/classificados e a versão atual de cada produto. Registros críticos inválidos seguem para uma tabela de quarentena; não desaparecem silenciosamente.

# COMMAND ----------

# DBTITLE 1,Transformar dados de pedidos com tipos e classificações
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW orders_typed_ranked AS
# MAGIC WITH typed AS (
# MAGIC     SELECT
# MAGIC         UPPER(NULLIF(TRIM(Order_ID), '')) AS order_id,
# MAGIC         UPPER(NULLIF(TRIM(Product_ID), '')) AS product_id,
# MAGIC         UPPER(NULLIF(TRIM(Customer_ID), '')) AS customer_id,
# MAGIC         CAST(COALESCE(
# MAGIC             TRY_TO_TIMESTAMP(TRIM(Order_Date), 'yyyy-MM-dd'),
# MAGIC             TRY_TO_TIMESTAMP(TRIM(Order_Date), 'dd-MM-yyyy'),
# MAGIC             TRY_TO_TIMESTAMP(TRIM(Order_Date), 'dd/MM/yyyy')
# MAGIC         ) AS DATE) AS order_date,
# MAGIC         COALESCE(
# MAGIC     TRY_TO_TIMESTAMP(
# MAGIC         CONCAT(TRIM(Order_Date), ' ', TRIM(Order_Time)),
# MAGIC         'yyyy-MM-dd HH:mm:ss'
# MAGIC     ),
# MAGIC     TRY_TO_TIMESTAMP(
# MAGIC         CONCAT(TRIM(Order_Date), ' ', TRIM(Order_Time)),
# MAGIC         'dd-MM-yyyy HH:mm:ss'
# MAGIC     ),
# MAGIC     TRY_TO_TIMESTAMP(
# MAGIC         CONCAT(TRIM(Order_Date), ' ', TRIM(Order_Time)),
# MAGIC         'dd/MM/yyyy HH:mm:ss'
# MAGIC     )
# MAGIC ) AS order_timestamp,
# MAGIC         CAST(COALESCE(
# MAGIC             TRY_TO_TIMESTAMP(TRIM(Delivery_Date), 'yyyy-MM-dd'),
# MAGIC             TRY_TO_TIMESTAMP(TRIM(Delivery_Date), 'dd-MM-yyyy'),
# MAGIC             TRY_TO_TIMESTAMP(TRIM(Delivery_Date), 'dd/MM/yyyy')
# MAGIC         ) AS DATE) AS delivery_date,
# MAGIC         TRY_CAST(TRIM(Quantity) AS INT) AS quantity,
# MAGIC         TRY_CAST(TRIM(Unit_Price) AS DECIMAL(18,2)) AS unit_price,
# MAGIC         TRY_CAST(TRIM(Order_Value) AS DECIMAL(18,2)) AS order_value,
# MAGIC         TRY_CAST(TRIM(Shipping_Cost) AS DECIMAL(18,2)) AS shipping_amount,
# MAGIC         NULLIF(UPPER(TRIM(Coupon_Code)), '') AS coupon_code,
# MAGIC         TRY_CAST(TRIM(Coupon_Discount) AS DECIMAL(18,2)) AS coupon_discount,
# MAGIC         TRY_CAST(TRIM(Total_Amount) AS DECIMAL(18,2)) AS total_amount,
# MAGIC         COALESCE(NULLIF(UPPER(TRIM(Payment_Mode)), ''), 'NAO_INFORMADO') AS payment_mode,
# MAGIC         COALESCE(NULLIF(UPPER(TRIM(Order_Status)), ''), 'NAO_INFORMADO') AS order_status,
# MAGIC         CASE
# MAGIC             WHEN TRY_CAST(TRIM(Rating) AS DECIMAL(3,2)) BETWEEN 1 AND 5
# MAGIC             THEN TRY_CAST(TRIM(Rating) AS DECIMAL(3,2))
# MAGIC         END AS rating,
# MAGIC         NULLIF(TRIM(Review_Text), '') AS review_text,
# MAGIC         COALESCE(NULLIF(INITCAP(TRIM(City)), ''), 'Nao Informado') AS city,
# MAGIC         COALESCE(NULLIF(UPPER(TRIM(State)), ''), 'NAO_INFORMADO') AS state,
# MAGIC         CASE
# MAGIC             WHEN TRY_CAST(TRIM(Customer_Age) AS INT) BETWEEN 18 AND 120
# MAGIC             THEN TRY_CAST(TRIM(Customer_Age) AS INT)
# MAGIC         END AS customer_age,
# MAGIC         COALESCE(NULLIF(UPPER(TRIM(Customer_Age_Group)), ''), 'NAO_INFORMADO') AS customer_age_group,
# MAGIC         _rescued_data,
# MAGIC         ingestion_timestamp,
# MAGIC         ingestion_date,
# MAGIC         source_file_name,
# MAGIC         source_file_path,
# MAGIC         source_file_modification_time
# MAGIC     FROM rower_bootcamp.bronze.orders_raw
# MAGIC ), classified AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE
# MAGIC             WHEN order_id IS NOT NULL AND product_id IS NOT NULL
# MAGIC             THEN SHA2(CONCAT_WS('||', order_id, product_id), 256)
# MAGIC         END AS sales_line_key,
# MAGIC         SHA2(CONCAT_WS('||',
# MAGIC             COALESCE(order_id, '__NULL__'),
# MAGIC             COALESCE(product_id, '__NULL__'),
# MAGIC             COALESCE(customer_id, '__NULL__'),
# MAGIC             COALESCE(CAST(order_timestamp AS STRING), '__NULL__'),
# MAGIC             COALESCE(CAST(quantity AS STRING), '__NULL__'),
# MAGIC             COALESCE(CAST(total_amount AS STRING), '__NULL__'),
# MAGIC             COALESCE(source_file_path, '__NULL__'),
# MAGIC             COALESCE(CAST(source_file_modification_time AS STRING), '__NULL__')
# MAGIC         ), 256) AS source_record_hash,
# MAGIC         NULLIF(CONCAT_WS(';',
# MAGIC             CASE WHEN order_id IS NULL THEN 'ORDER_ID_NULO' END,
# MAGIC             CASE WHEN product_id IS NULL THEN 'PRODUCT_ID_NULO' END,
# MAGIC             CASE WHEN order_date IS NULL THEN 'ORDER_DATE_INVALIDA' END,
# MAGIC             CASE WHEN quantity IS NULL OR quantity <= 0 THEN 'QUANTIDADE_INVALIDA' END,
# MAGIC             CASE WHEN total_amount IS NULL OR total_amount < 0 THEN 'TOTAL_AMOUNT_INVALIDO' END
# MAGIC         ), '') AS invalid_reason
# MAGIC     FROM typed
# MAGIC ), labeled AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         COALESCE(sales_line_key, source_record_hash) AS deduplication_key
# MAGIC     FROM classified
# MAGIC )
# MAGIC SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC         PARTITION BY deduplication_key
# MAGIC         ORDER BY source_file_modification_time DESC, ingestion_timestamp DESC, source_file_path DESC
# MAGIC     ) AS version_rank
# MAGIC FROM labeled;

# COMMAND ----------

# DBTITLE 1,Criar visualização temporária de produtos com dados lim ...
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW products_typed_current AS
# MAGIC WITH typed AS (
# MAGIC     SELECT
# MAGIC         UPPER(NULLIF(TRIM(Product_ID), '')) AS product_id,
# MAGIC         COALESCE(NULLIF(TRIM(Product_Name), ''), 'Nao Informado') AS product_name,
# MAGIC         COALESCE(NULLIF(UPPER(TRIM(Category)), ''), 'NAO_INFORMADO') AS category,
# MAGIC         COALESCE(NULLIF(UPPER(TRIM(Brand)), ''), 'NAO_INFORMADO') AS brand,
# MAGIC         TRY_CAST(TRIM(Original_Price) AS DECIMAL(18,2)) AS original_price,
# MAGIC         TRY_CAST(TRIM(Discount_Percent) AS DECIMAL(7,2)) AS catalog_discount_percentage,
# MAGIC         TRY_CAST(TRIM(Discount_Amount) AS DECIMAL(18,2)) AS catalog_discount_amount,
# MAGIC         TRY_CAST(TRIM(Selling_Price) AS DECIMAL(18,2)) AS selling_price,
# MAGIC         TRY_CAST(TRIM(Stock_Quantity) AS INT) AS stock_quantity,
# MAGIC         TRY_CAST(TRIM(Weight_kg) AS DECIMAL(10,3)) AS weight_kg,
# MAGIC         TRY_CAST(TRIM(Avg_Rating) AS DECIMAL(3,2)) AS average_product_rating,
# MAGIC         TRY_CAST(TRIM(Total_Reviews) AS BIGINT) AS total_reviews,
# MAGIC         _rescued_data,
# MAGIC         ingestion_timestamp,
# MAGIC         source_file_name,
# MAGIC         source_file_path,
# MAGIC         source_file_modification_time,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY UPPER(NULLIF(TRIM(Product_ID), ''))
# MAGIC             ORDER BY source_file_modification_time DESC, ingestion_timestamp DESC, source_file_path DESC
# MAGIC         ) AS version_rank
# MAGIC     FROM rower_bootcamp.bronze.products_raw
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM typed
# MAGIC WHERE product_id IS NOT NULL AND version_rank = 1;

# COMMAND ----------

# DBTITLE 1,Registrar pedidos rejeitados com motivo e rastreabilida ...
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE rower_bootcamp.silver.sales_quarantine
# MAGIC COMMENT 'Registros de pedidos rejeitados na Silver, com motivo e rastreabilidade'
# MAGIC AS
# MAGIC SELECT * EXCEPT (version_rank, deduplication_key)
# MAGIC FROM orders_typed_ranked
# MAGIC WHERE version_rank = 1
# MAGIC   AND invalid_reason IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,Criar tabela de vendas deduplicadas e enriquecidas
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE rower_bootcamp.silver.sales
# MAGIC COMMENT 'Vendas válidas, tipadas, deduplicadas, enriquecidas e rastreáveis'
# MAGIC AS
# MAGIC WITH valid_orders AS (
# MAGIC     SELECT * EXCEPT (version_rank, invalid_reason, deduplication_key)
# MAGIC     FROM orders_typed_ranked
# MAGIC     WHERE version_rank = 1
# MAGIC       AND invalid_reason IS NULL
# MAGIC )
# MAGIC SELECT
# MAGIC     o.sales_line_key,
# MAGIC     o.order_id,
# MAGIC     o.product_id,
# MAGIC     o.customer_id,
# MAGIC     o.order_date,
# MAGIC     o.order_timestamp,
# MAGIC     o.delivery_date,
# MAGIC     YEAR(o.order_date) AS order_year,
# MAGIC     MONTH(o.order_date) AS order_month,
# MAGIC     QUARTER(o.order_date) AS order_quarter,
# MAGIC     DATE_FORMAT(o.order_date, 'yyyy-MM') AS year_month,
# MAGIC     CASE
# MAGIC         WHEN o.delivery_date >= o.order_date
# MAGIC         THEN DATEDIFF(o.delivery_date, o.order_date)
# MAGIC     END AS fulfillment_days,
# MAGIC     o.quantity,
# MAGIC     o.unit_price,
# MAGIC     o.order_value,
# MAGIC     o.shipping_amount,
# MAGIC     o.coupon_code,
# MAGIC     o.coupon_discount,
# MAGIC     o.total_amount,
# MAGIC     CASE
# MAGIC         WHEN o.order_value > 0
# MAGIC         THEN ROUND(COALESCE(o.coupon_discount, 0) / o.order_value * 100, 2)
# MAGIC     END AS coupon_discount_percentage,
# MAGIC     o.payment_mode,
# MAGIC     o.order_status,
# MAGIC     o.rating,
# MAGIC     o.review_text,
# MAGIC     o.city,
# MAGIC     o.state,
# MAGIC     o.customer_age,
# MAGIC     o.customer_age_group,
# MAGIC     COALESCE(p.product_name, 'Nao Informado') AS product_name,
# MAGIC     COALESCE(p.category, 'NAO_INFORMADO') AS category,
# MAGIC     COALESCE(p.brand, 'NAO_INFORMADO') AS brand,
# MAGIC     p.original_price,
# MAGIC     p.selling_price,
# MAGIC     p.catalog_discount_percentage,
# MAGIC     p.catalog_discount_amount,
# MAGIC     p.stock_quantity,
# MAGIC     p.weight_kg,
# MAGIC     p.average_product_rating,
# MAGIC     p.total_reviews,
# MAGIC     CASE WHEN p.product_id IS NULL THEN 'SEM_CADASTRO' ELSE 'CADASTRADO' END AS product_match_status,
# MAGIC     o.source_file_name AS order_source_file,
# MAGIC     o.source_file_path AS order_source_path,
# MAGIC     o.source_file_modification_time AS order_source_modified_at,
# MAGIC     o.ingestion_timestamp AS order_ingested_at,
# MAGIC     p.source_file_name AS product_source_file,
# MAGIC     p.source_file_path AS product_source_path,
# MAGIC     p.source_file_modification_time AS product_source_modified_at,
# MAGIC     p.ingestion_timestamp AS product_ingested_at,
# MAGIC     CURRENT_TIMESTAMP() AS silver_processed_at
# MAGIC FROM valid_orders o
# MAGIC LEFT JOIN products_typed_current p
# MAGIC     ON o.product_id = p.product_id;

# COMMAND ----------

# DBTITLE 1,Atualizar comentários das colunas na tabela de vendas
# MAGIC %sql
# MAGIC ALTER TABLE rower_bootcamp.silver.sales ALTER COLUMN sales_line_key
# MAGIC COMMENT 'Chave técnica SHA-256 formada por order_id e product_id';
# MAGIC ALTER TABLE rower_bootcamp.silver.sales ALTER COLUMN total_amount
# MAGIC COMMENT 'Valor final registrado na origem; não representa lucro';
# MAGIC ALTER TABLE rower_bootcamp.silver.sales ALTER COLUMN shipping_amount
# MAGIC COMMENT 'Valor de frete informado na origem, sem assumir se é custo interno ou cobrança';
# MAGIC ALTER TABLE rower_bootcamp.silver.sales ALTER COLUMN product_match_status
# MAGIC COMMENT 'Indica se o produto do pedido foi encontrado no cadastro de produtos';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Explicação das transformações
# MAGIC
# MAGIC 1. **Tipagem:** `TRY_CAST` converte números sem interromper a carga quando existe conteúdo inválido.
# MAGIC 2. **Datas:** são aceitos três formatos; data e hora do pedido também formam um timestamp.
# MAGIC 3. **Nulos e padronização:** identificadores são aparados e convertidos para maiúsculas; vazios tornam-se nulos; dimensões textuais recebem valores explícitos quando apropriado.
# MAGIC 4. **Deduplicação:** a versão atual é escolhida por `order_id + product_id`, nunca apenas por `order_id` sem validar o grão.
# MAGIC 5. **Inválidos:** registros críticos seguem para `sales_quarantine` com o motivo da rejeição.
# MAGIC 6. **Integração:** o `LEFT JOIN` preserva o pedido mesmo quando o produto não existe no cadastro.
# MAGIC 7. **Derivadas:** ano, mês, trimestre, tempo de entrega e percentual de desconto.
# MAGIC 8. **Sem falso lucro:** o dataset não fornece custo de aquisição/CMV. Portanto, `total_amount - shipping_amount` não é chamado de lucro.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Validações da Silver

# COMMAND ----------

# DBTITLE 1,Analisar dados de vendas e identificar registros inváli ...
# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS silver_rows,
# MAGIC     COUNT(DISTINCT sales_line_key) AS distinct_sales_line_keys,
# MAGIC     SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
# MAGIC     SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_ids,
# MAGIC     SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS null_order_dates,
# MAGIC     SUM(CASE WHEN total_amount IS NULL OR total_amount < 0 THEN 1 ELSE 0 END) AS invalid_amounts,
# MAGIC     SUM(CASE WHEN quantity IS NULL OR quantity <= 0 THEN 1 ELSE 0 END) AS invalid_quantities,
# MAGIC     SUM(CASE WHEN product_match_status = 'SEM_CADASTRO' THEN 1 ELSE 0 END) AS unmatched_products
# MAGIC FROM rower_bootcamp.silver.sales;

# COMMAND ----------

# DBTITLE 1,Identificar duplicatas nas vendas do catálogo rower_boo ...
# MAGIC %sql
# MAGIC SELECT sales_line_key, COUNT(*) AS occurrences
# MAGIC FROM rower_bootcamp.silver.sales
# MAGIC GROUP BY sales_line_key
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# DBTITLE 1,Analisar motivos de rejeição e contagem de linhas invál ...
# MAGIC %sql
# MAGIC SELECT invalid_reason, COUNT(*) AS rejected_rows
# MAGIC FROM rower_bootcamp.silver.sales_quarantine
# MAGIC GROUP BY invalid_reason
# MAGIC ORDER BY rejected_rows DESC;

# COMMAND ----------

# DBTITLE 1,Analisar contagem de linhas e status de validação sales
# MAGIC %sql
# MAGIC WITH bronze_current AS (
# MAGIC     SELECT COUNT(*) AS rows_after_latest_version
# MAGIC     FROM orders_typed_ranked
# MAGIC     WHERE version_rank = 1
# MAGIC ), silver_and_quarantine AS (
# MAGIC     SELECT
# MAGIC         (SELECT COUNT(*) FROM rower_bootcamp.silver.sales) +
# MAGIC         (SELECT COUNT(*) FROM rower_bootcamp.silver.sales_quarantine) AS classified_rows
# MAGIC )
# MAGIC SELECT
# MAGIC     b.rows_after_latest_version,
# MAGIC     s.classified_rows,
# MAGIC     b.rows_after_latest_version - s.classified_rows AS difference,
# MAGIC     CASE WHEN b.rows_after_latest_version = s.classified_rows THEN 'APROVADO' ELSE 'VERIFICAR' END AS status
# MAGIC FROM bronze_current b CROSS JOIN silver_and_quarantine s;

# COMMAND ----------

# DBTITLE 1,Análise de consistência nos valores financeiros de vend ...
# MAGIC %sql
# MAGIC -- Diagnóstico de consistência financeira
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_records,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN ABS(
# MAGIC                 total_amount -
# MAGIC                 (
# MAGIC                     order_value
# MAGIC                     - COALESCE(coupon_discount, 0)
# MAGIC                     + COALESCE(shipping_amount, 0)
# MAGIC                 )
# MAGIC             ) > 0.01
# MAGIC             THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS inconsistent_financial_values,
# MAGIC
# MAGIC     ROUND(
# MAGIC         MAX(
# MAGIC             ABS(
# MAGIC                 total_amount -
# MAGIC                 (
# MAGIC                     order_value
# MAGIC                     - COALESCE(coupon_discount, 0)
# MAGIC                     + COALESCE(shipping_amount, 0)
# MAGIC                 )
# MAGIC             )
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS maximum_difference
# MAGIC
# MAGIC FROM rower_bootcamp.silver.sales;

# COMMAND ----------

# DBTITLE 1,Avaliar a qualidade e integridade dos dados de pedidos
# MAGIC %sql
# MAGIC WITH silver_quality AS (
# MAGIC     SELECT
# MAGIC         -- Conta registros sem data e hora do pedido.
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN order_timestamp IS NULL THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS null_order_timestamps,
# MAGIC
# MAGIC         -- Identifica entregas com data anterior à data do pedido.
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN delivery_date < order_date THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS delivery_before_order,
# MAGIC
# MAGIC         -- Identifica pedidos entregues sem data de entrega.
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN order_status = 'DELIVERED'
# MAGIC                  AND delivery_date IS NULL
# MAGIC                 THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS delivered_without_delivery_date,
# MAGIC
# MAGIC         -- Verifica se existem status diferentes dos cinco aceitos.
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN order_status NOT IN (
# MAGIC                     'DELIVERED',
# MAGIC                     'SHIPPED',
# MAGIC                     'CANCELLED',
# MAGIC                     'RETURNED',
# MAGIC                     'PROCESSING'
# MAGIC                 )
# MAGIC                 THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS rows_with_unknown_status
# MAGIC
# MAGIC     FROM rower_bootcamp.silver.sales
# MAGIC ),
# MAGIC
# MAGIC order_status_quality AS (
# MAGIC     SELECT
# MAGIC         COUNT(*) AS orders_with_multiple_statuses
# MAGIC
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             order_id
# MAGIC
# MAGIC         FROM rower_bootcamp.silver.sales
# MAGIC
# MAGIC         GROUP BY
# MAGIC             order_id
# MAGIC
# MAGIC         HAVING COUNT(DISTINCT order_status) > 1
# MAGIC     )
# MAGIC ),
# MAGIC
# MAGIC order_segment_quality AS (
# MAGIC     SELECT
# MAGIC         COUNT(*) AS orders_with_multiple_segments
# MAGIC
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             order_id
# MAGIC
# MAGIC         FROM rower_bootcamp.silver.sales
# MAGIC
# MAGIC         GROUP BY
# MAGIC             order_id
# MAGIC
# MAGIC         HAVING COUNT(
# MAGIC             DISTINCT CONCAT_WS(
# MAGIC                 '||',
# MAGIC                 state,
# MAGIC                 customer_age_group
# MAGIC             )
# MAGIC         ) > 1
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     q.null_order_timestamps,
# MAGIC     q.delivery_before_order,
# MAGIC     q.delivered_without_delivery_date,
# MAGIC     q.rows_with_unknown_status,
# MAGIC     s.orders_with_multiple_statuses,
# MAGIC     g.orders_with_multiple_segments,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN q.null_order_timestamps = 0
# MAGIC          AND q.delivery_before_order = 0
# MAGIC          AND q.delivered_without_delivery_date = 0
# MAGIC          AND q.rows_with_unknown_status = 0
# MAGIC          AND s.orders_with_multiple_statuses = 0
# MAGIC          AND g.orders_with_multiple_segments = 0
# MAGIC         THEN 'APROVADO'
# MAGIC         ELSE 'VERIFICAR'
# MAGIC     END AS quality_status
# MAGIC
# MAGIC FROM silver_quality q
# MAGIC
# MAGIC CROSS JOIN order_status_quality s
# MAGIC
# MAGIC CROSS JOIN order_segment_quality g;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Criação da primeira Gold — indicadores mensais
# MAGIC
# MAGIC Receita reconhecida considera somente pedidos `DELIVERED`. Pedidos cancelados, devolvidos e ainda em processamento aparecem como indicadores separados.

# COMMAND ----------

# DBTITLE 1,Criar tabela de indicadores mensais de vendas e entrega ...
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE rower_bootcamp.gold.monthly_sales_indicators
# MAGIC COMMENT 'Indicadores mensais de pedidos, receita reconhecida, descontos e entrega'
# MAGIC AS
# MAGIC
# MAGIC -- Consolida as linhas de produtos no nível do pedido.
# MAGIC WITH order_level AS (
# MAGIC     SELECT
# MAGIC         order_id,
# MAGIC         order_year,
# MAGIC         order_month,
# MAGIC         year_month,
# MAGIC
# MAGIC         -- A validação da Silver confirma um único status por pedido.
# MAGIC         MAX(order_status) AS order_status,
# MAGIC
# MAGIC         -- Soma todas as unidades do pedido.
# MAGIC         SUM(quantity) AS order_units,
# MAGIC
# MAGIC         -- Soma o valor das linhas do pedido.
# MAGIC         SUM(total_amount) AS order_total_amount,
# MAGIC
# MAGIC         -- Soma o desconto de cupom das linhas.
# MAGIC         SUM(
# MAGIC             COALESCE(coupon_discount, 0)
# MAGIC         ) AS order_coupon_discount,
# MAGIC
# MAGIC         -- Prazo de atendimento do pedido.
# MAGIC         MAX(fulfillment_days) AS fulfillment_days,
# MAGIC
# MAGIC         -- Média das avaliações das linhas do pedido.
# MAGIC         AVG(rating) AS order_rating
# MAGIC
# MAGIC     FROM rower_bootcamp.silver.sales
# MAGIC
# MAGIC     GROUP BY
# MAGIC         order_id,
# MAGIC         order_year,
# MAGIC         order_month,
# MAGIC         year_month
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     order_year,
# MAGIC     order_month,
# MAGIC     year_month,
# MAGIC
# MAGIC     -- Como a CTE anterior possui uma linha por pedido,
# MAGIC     -- COUNT(*) representa a quantidade de pedidos.
# MAGIC     COUNT(*) AS total_orders,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN order_status = 'DELIVERED' THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS delivered_orders,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN order_status = 'SHIPPED' THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS shipped_orders,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN order_status = 'PROCESSING' THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS processing_orders,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN order_status = 'CANCELLED' THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS cancelled_orders,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN order_status = 'RETURNED' THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS returned_orders,
# MAGIC
# MAGIC     -- Unidades somente dos pedidos entregues.
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN order_status = 'DELIVERED'
# MAGIC             THEN order_units
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS delivered_units,
# MAGIC
# MAGIC     -- Receita reconhecida somente após a entrega.
# MAGIC     ROUND(
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN order_status = 'DELIVERED'
# MAGIC                 THEN order_total_amount
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS recognized_revenue,
# MAGIC
# MAGIC     -- Desconto associado aos pedidos entregues.
# MAGIC     ROUND(
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN order_status = 'DELIVERED'
# MAGIC                 THEN order_coupon_discount
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS delivered_coupon_discount,
# MAGIC
# MAGIC     -- Valor médio dos pedidos entregues.
# MAGIC     ROUND(
# MAGIC         AVG(
# MAGIC             CASE
# MAGIC                 WHEN order_status = 'DELIVERED'
# MAGIC                 THEN order_total_amount
# MAGIC             END
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS average_delivered_order_value,
# MAGIC
# MAGIC     -- Prazo médio dos pedidos entregues.
# MAGIC     ROUND(
# MAGIC         AVG(
# MAGIC             CASE
# MAGIC                 WHEN order_status = 'DELIVERED'
# MAGIC                 THEN fulfillment_days
# MAGIC             END
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS average_fulfillment_days,
# MAGIC
# MAGIC     -- Avaliação média dos pedidos entregues.
# MAGIC     ROUND(
# MAGIC         AVG(
# MAGIC             CASE
# MAGIC                 WHEN order_status = 'DELIVERED'
# MAGIC                 THEN order_rating
# MAGIC             END
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS average_delivered_rating,
# MAGIC
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC
# MAGIC FROM order_level
# MAGIC
# MAGIC GROUP BY
# MAGIC     order_year,
# MAGIC     order_month,
# MAGIC     year_month;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Criação da segunda Gold — ranking de marcas

# COMMAND ----------

# DBTITLE 1,Rankear marcas por receita de pedidos entregues
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE rower_bootcamp.gold.brand_sales_ranking
# MAGIC COMMENT 'Ranking global de marcas por receita reconhecida de pedidos entregues'
# MAGIC AS
# MAGIC WITH brand_metrics AS (
# MAGIC     SELECT
# MAGIC         category,
# MAGIC         brand,
# MAGIC         COUNT(DISTINCT order_id) AS delivered_orders,
# MAGIC         SUM(quantity) AS delivered_units,
# MAGIC         ROUND(SUM(total_amount), 2) AS recognized_revenue,
# MAGIC         ROUND(SUM(COALESCE(coupon_discount, 0)), 2) AS discount_amount,
# MAGIC         ROUND(AVG(total_amount), 2) AS average_sales_line_value
# MAGIC     FROM rower_bootcamp.silver.sales
# MAGIC     WHERE order_status = 'DELIVERED'
# MAGIC     GROUP BY category, brand
# MAGIC )
# MAGIC SELECT
# MAGIC     category,
# MAGIC     brand,
# MAGIC     delivered_orders,
# MAGIC     delivered_units,
# MAGIC     recognized_revenue,
# MAGIC     discount_amount,
# MAGIC     average_sales_line_value,
# MAGIC     ROUND(recognized_revenue / NULLIF(SUM(recognized_revenue) OVER (), 0) * 100, 2) AS revenue_share_percentage,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC     ORDER BY
# MAGIC         recognized_revenue DESC,
# MAGIC         category ASC,
# MAGIC         brand ASC
# MAGIC ) AS revenue_rank,
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC FROM brand_metrics;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Explicação do valor de negócio das Golds
# MAGIC
# MAGIC - **Indicadores mensais:** permitem acompanhar volume, receita efetivamente entregue, cancelamentos, devoluções, descontos e prazo de entrega.
# MAGIC - **Ranking de marcas:** identifica as marcas com maior contribuição de receita e sua participação percentual no total entregue.
# MAGIC
# MAGIC A alteração mais importante em relação ao notebook anterior é não chamar de “lucro” uma subtração que não contém CMV/custo do produto. Para calcular lucro verdadeiro seria necessário incorporar uma fonte confiável de custo de aquisição.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unidade monetária
# MAGIC
# MAGIC Os valores monetários foram preservados na unidade apresentada pelo dataset.
# MAGIC A moeda precisa ser confirmada na documentação oficial da fonte antes do uso
# MAGIC dos indicadores em relatórios financeiros.

# COMMAND ----------

# DBTITLE 1,Validar integridade e consistência das Golds de vendas
# MAGIC %sql
# MAGIC -- validações específicas das Golds
# MAGIC
# MAGIC WITH monthly_quality AS (
# MAGIC     SELECT
# MAGIC         -- Quantidade de linhas da Gold mensal.
# MAGIC         COUNT(*) AS monthly_rows,
# MAGIC
# MAGIC         -- Quantidade de períodos diferentes.
# MAGIC         COUNT(DISTINCT year_month) AS distinct_months,
# MAGIC
# MAGIC         -- Períodos sem identificação.
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN year_month IS NULL THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS null_periods,
# MAGIC
# MAGIC         -- Receita mensal negativa.
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN recognized_revenue < 0 THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS negative_monthly_revenue,
# MAGIC
# MAGIC         -- Verifica se a soma dos status corresponde ao total.
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN total_orders <>
# MAGIC                     delivered_orders
# MAGIC                     + shipped_orders
# MAGIC                     + processing_orders
# MAGIC                     + cancelled_orders
# MAGIC                      + returned_orders
# MAGIC                 THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS months_with_status_difference
# MAGIC
# MAGIC     FROM rower_bootcamp.gold.monthly_sales_indicators
# MAGIC ),
# MAGIC
# MAGIC ranking_quality AS (
# MAGIC     SELECT
# MAGIC         -- Quantidade de linhas do ranking.
# MAGIC         COUNT(*) AS ranking_rows,
# MAGIC
# MAGIC         -- Combinações diferentes de categoria e marca.
# MAGIC         COUNT(
# MAGIC             DISTINCT CONCAT_WS(
# MAGIC                 '||',
# MAGIC                 category,
# MAGIC                 brand
# MAGIC             )
# MAGIC         ) AS distinct_category_brands,
# MAGIC
# MAGIC         -- Receita negativa no ranking.
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN recognized_revenue < 0 THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS negative_ranking_revenue,
# MAGIC
# MAGIC         -- A participação total deve ficar próxima de 100%.
# MAGIC         ROUND(
# MAGIC             SUM(revenue_share_percentage),
# MAGIC             2
# MAGIC         ) AS revenue_share_total
# MAGIC
# MAGIC     FROM rower_bootcamp.gold.brand_sales_ranking
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     m.*,
# MAGIC     r.*,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN m.monthly_rows = m.distinct_months
# MAGIC          AND m.null_periods = 0
# MAGIC          AND m.negative_monthly_revenue = 0
# MAGIC          AND m.months_with_status_difference = 0
# MAGIC          AND r.ranking_rows = r.distinct_category_brands
# MAGIC          AND r.negative_ranking_revenue = 0
# MAGIC          AND ABS(r.revenue_share_total - 100) <= 0.20
# MAGIC         THEN 'APROVADO'
# MAGIC         ELSE 'VERIFICAR'
# MAGIC     END AS gold_quality_status
# MAGIC
# MAGIC FROM monthly_quality m
# MAGIC CROSS JOIN ranking_quality r;

# COMMAND ----------

# MAGIC %md
# MAGIC # Tabela Gold bônus - desempenho por segmento regional e etário
# MAGIC
# MAGIC ## Por que esta terceira Gold é necessária?
# MAGIC
# MAGIC O projeto já possui duas perspectivas analíticas:
# MAGIC
# MAGIC 1. `monthly_sales_indicators`: acompanha vendas e receita ao longo do tempo;
# MAGIC 2. `brand_sales_ranking`: identifica as marcas com maior contribuição de receita.
# MAGIC
# MAGIC A tabela `customer_segment_performance` acrescenta a perspectiva de **cliente e localização**, consolidando os resultados por `state` e `customer_age_group`.
# MAGIC
# MAGIC ## Decisão de negócio apoiada
# MAGIC
# MAGIC A nova Gold apoia a **priorização de campanhas e investimentos por segmento**. Ela permite identificar grupos com maior receita, adesão a cupons, cancelamentos, devoluções e possíveis problemas de entrega.
# MAGIC
# MAGIC A tabela não calcula lucro, pois o dataset não possui CMV ou custo de aquisição. Os valores monetários permanecem na unidade apresentada pela fonte.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Granularidade e regras
# MAGIC
# MAGIC **Granularidade:** uma linha por combinação de estado e faixa etária.
# MAGIC
# MAGIC * Receita reconhecida considera somente pedidos `DELIVERED`;
# MAGIC * `SHIPPED`, `PROCESSING`, `CANCELLED` e `RETURNED` são medidos separadamente;
# MAGIC * Os dados são primeiro consolidados no nível do pedido;
# MAGIC * O uso de cupom considera código preenchido ou desconto de cupom maior que zero;
# MAGIC * `NULLIF` evita divisão por zero;
# MAGIC * `CREATE OR REPLACE TABLE` segue o padrão de reprocessamento do projeto.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Criar tabela segmentada de desempenho comercial por est ...
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE rower_bootcamp.gold.customer_segment_performance
# MAGIC COMMENT 'Desempenho comercial por estado e faixa etária para priorização de campanhas'
# MAGIC AS
# MAGIC
# MAGIC -- Parâmetros utilizados pela regra de negócio.
# MAGIC WITH parameters AS (
# MAGIC     SELECT
# MAGIC         15 AS priority_rank_limit,
# MAGIC         CAST(10.00 AS DECIMAL(5,2)) AS maximum_operational_loss_rate
# MAGIC ),
# MAGIC
# MAGIC -- Consolida as linhas de produtos no nível do pedido.
# MAGIC order_level AS (
# MAGIC     SELECT
# MAGIC         order_id,
# MAGIC
# MAGIC         -- Evita dimensões nulas.
# MAGIC         COALESCE(state, 'NAO_INFORMADO') AS state,
# MAGIC         COALESCE(
# MAGIC             customer_age_group,
# MAGIC             'NAO_INFORMADO'
# MAGIC         ) AS customer_age_group,
# MAGIC
# MAGIC         -- A validação Silver confirma que o pedido possui um único status.
# MAGIC         MAX(order_status) AS order_status,
# MAGIC
# MAGIC         -- Quantidade total de unidades do pedido.
# MAGIC         SUM(quantity) AS order_units,
# MAGIC
# MAGIC         -- Valor total do pedido.
# MAGIC         SUM(total_amount) AS order_total_amount,
# MAGIC
# MAGIC         -- Desconto total aplicado ao pedido.
# MAGIC         SUM(
# MAGIC             COALESCE(coupon_discount, 0)
# MAGIC         ) AS order_coupon_discount,
# MAGIC
# MAGIC         -- Indica se pelo menos uma linha do pedido usou cupom.
# MAGIC         MAX(
# MAGIC             CASE
# MAGIC                 WHEN coupon_code IS NOT NULL
# MAGIC                   OR COALESCE(coupon_discount, 0) > 0
# MAGIC                 THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS used_coupon,
# MAGIC
# MAGIC         -- Avaliação média do pedido.
# MAGIC         AVG(rating) AS order_rating,
# MAGIC
# MAGIC         -- Tempo de atendimento do pedido.
# MAGIC         MAX(fulfillment_days) AS fulfillment_days
# MAGIC
# MAGIC     FROM rower_bootcamp.silver.sales
# MAGIC
# MAGIC     GROUP BY
# MAGIC         order_id,
# MAGIC         COALESCE(state, 'NAO_INFORMADO'),
# MAGIC         COALESCE(
# MAGIC             customer_age_group,
# MAGIC             'NAO_INFORMADO'
# MAGIC         )
# MAGIC ),
# MAGIC
# MAGIC -- Agrega os pedidos por estado e faixa etária.
# MAGIC segment_metrics AS (
# MAGIC     SELECT
# MAGIC         state,
# MAGIC         customer_age_group,
# MAGIC
# MAGIC         -- Cada linha da CTE anterior representa um pedido.
# MAGIC         COUNT(*) AS total_orders,
# MAGIC
# MAGIC         -- Quantidade de pedidos em cada status.
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN order_status = 'DELIVERED' THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS delivered_orders,
# MAGIC
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN order_status = 'SHIPPED' THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS shipped_orders,
# MAGIC
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN order_status = 'PROCESSING' THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS processing_orders,
# MAGIC
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN order_status = 'CANCELLED' THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS cancelled_orders,
# MAGIC
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN order_status = 'RETURNED' THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS returned_orders,
# MAGIC
# MAGIC         -- Unidades associadas a pedidos entregues.
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN order_status = 'DELIVERED'
# MAGIC                 THEN order_units
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS delivered_units,
# MAGIC
# MAGIC         -- Receita reconhecida somente após a entrega.
# MAGIC         ROUND(
# MAGIC             SUM(
# MAGIC                 CASE
# MAGIC                     WHEN order_status = 'DELIVERED'
# MAGIC                     THEN order_total_amount
# MAGIC                     ELSE 0
# MAGIC                 END
# MAGIC             ),
# MAGIC             2
# MAGIC         ) AS recognized_revenue,
# MAGIC
# MAGIC         -- Descontos associados aos pedidos entregues.
# MAGIC         ROUND(
# MAGIC             SUM(
# MAGIC                 CASE
# MAGIC                     WHEN order_status = 'DELIVERED'
# MAGIC                     THEN order_coupon_discount
# MAGIC                     ELSE 0
# MAGIC                 END
# MAGIC             ),
# MAGIC             2
# MAGIC         ) AS delivered_coupon_discount,
# MAGIC
# MAGIC         -- Valor médio dos pedidos entregues.
# MAGIC         ROUND(
# MAGIC             AVG(
# MAGIC                 CASE
# MAGIC                     WHEN order_status = 'DELIVERED'
# MAGIC                     THEN order_total_amount
# MAGIC                 END
# MAGIC             ),
# MAGIC             2
# MAGIC         ) AS average_delivered_order_value,
# MAGIC
# MAGIC         -- Tempo médio de atendimento dos pedidos entregues.
# MAGIC         ROUND(
# MAGIC             AVG(
# MAGIC                 CASE
# MAGIC                     WHEN order_status = 'DELIVERED'
# MAGIC                     THEN fulfillment_days
# MAGIC                 END
# MAGIC             ),
# MAGIC             2
# MAGIC         ) AS average_fulfillment_days,
# MAGIC
# MAGIC         -- Avaliação média dos pedidos entregues.
# MAGIC         ROUND(
# MAGIC             AVG(
# MAGIC                 CASE
# MAGIC                     WHEN order_status = 'DELIVERED'
# MAGIC                     THEN order_rating
# MAGIC                 END
# MAGIC             ),
# MAGIC             2
# MAGIC         ) AS average_delivered_rating,
# MAGIC
# MAGIC         -- Pedidos que utilizaram cupom.
# MAGIC         SUM(used_coupon) AS coupon_orders
# MAGIC
# MAGIC     FROM order_level
# MAGIC
# MAGIC     GROUP BY
# MAGIC         state,
# MAGIC         customer_age_group
# MAGIC ),
# MAGIC
# MAGIC -- Calcula os indicadores percentuais.
# MAGIC calculated_metrics AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC
# MAGIC         ROUND(
# MAGIC             delivered_orders * 100.0
# MAGIC             / NULLIF(total_orders, 0),
# MAGIC             2
# MAGIC         ) AS delivery_rate_percentage,
# MAGIC
# MAGIC         ROUND(
# MAGIC             cancelled_orders * 100.0
# MAGIC             / NULLIF(total_orders, 0),
# MAGIC             2
# MAGIC         ) AS cancellation_rate_percentage,
# MAGIC
# MAGIC         ROUND(
# MAGIC             returned_orders * 100.0
# MAGIC             / NULLIF(total_orders, 0),
# MAGIC             2
# MAGIC         ) AS return_rate_percentage,
# MAGIC
# MAGIC         ROUND(
# MAGIC             coupon_orders * 100.0
# MAGIC             / NULLIF(total_orders, 0),
# MAGIC             2
# MAGIC         ) AS coupon_usage_rate_percentage
# MAGIC
# MAGIC     FROM segment_metrics
# MAGIC ),
# MAGIC
# MAGIC -- Calcula participação na receita e posição no ranking.
# MAGIC ranked_metrics AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC
# MAGIC         ROUND(
# MAGIC             recognized_revenue
# MAGIC             / NULLIF(
# MAGIC                 SUM(recognized_revenue) OVER (),
# MAGIC                 0
# MAGIC             ) * 100,
# MAGIC             2
# MAGIC         ) AS revenue_share_percentage,
# MAGIC
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             ORDER BY
# MAGIC                 recognized_revenue DESC,
# MAGIC                 state ASC,
# MAGIC                 customer_age_group ASC
# MAGIC         ) AS revenue_rank
# MAGIC
# MAGIC     FROM calculated_metrics
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     r.state,
# MAGIC     r.customer_age_group,
# MAGIC     r.total_orders,
# MAGIC     r.delivered_orders,
# MAGIC     r.shipped_orders,
# MAGIC     r.processing_orders,
# MAGIC     r.cancelled_orders,
# MAGIC     r.returned_orders,
# MAGIC     r.delivered_units,
# MAGIC     r.recognized_revenue,
# MAGIC     r.delivered_coupon_discount,
# MAGIC     r.average_delivered_order_value,
# MAGIC     r.average_fulfillment_days,
# MAGIC     r.average_delivered_rating,
# MAGIC     r.coupon_orders,
# MAGIC     r.delivery_rate_percentage,
# MAGIC     r.cancellation_rate_percentage,
# MAGIC     r.return_rate_percentage,
# MAGIC     r.coupon_usage_rate_percentage,
# MAGIC     r.revenue_share_percentage,
# MAGIC     r.revenue_rank,
# MAGIC
# MAGIC     -- Regra final de recomendação.
# MAGIC     CASE
# MAGIC         WHEN r.revenue_rank <= p.priority_rank_limit
# MAGIC          AND (
# MAGIC              r.cancellation_rate_percentage
# MAGIC              + r.return_rate_percentage
# MAGIC          ) <= p.maximum_operational_loss_rate
# MAGIC         THEN 'PRIORIZAR_CAMPANHAS'
# MAGIC
# MAGIC         WHEN r.revenue_rank <= p.priority_rank_limit
# MAGIC          AND (
# MAGIC              r.cancellation_rate_percentage
# MAGIC              + r.return_rate_percentage
# MAGIC          ) > p.maximum_operational_loss_rate
# MAGIC         THEN 'REVISAR_EXPERIENCIA'
# MAGIC
# MAGIC         ELSE 'MONITORAR'
# MAGIC     END AS recommended_action,
# MAGIC
# MAGIC     CURRENT_TIMESTAMP() AS gold_processed_at
# MAGIC
# MAGIC FROM ranked_metrics r
# MAGIC CROSS JOIN parameters p;

# COMMAND ----------

# DBTITLE 1,Analisar desempenho de segmentos de clientes ordenados  ...
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM rower_bootcamp.gold.customer_segment_performance
# MAGIC ORDER BY revenue_rank;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação estrutural e de qualidade
# MAGIC
# MAGIC A validação verifica unicidade da granularidade, nulos, valores negativos, percentuais inválidos e a soma aproximada de 100% da participação na receita.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Verificar integridade e consistência dos segmentos e re ...
# MAGIC %sql
# MAGIC WITH segment_quality AS (
# MAGIC     SELECT
# MAGIC         -- Quantidade de linhas da tabela.
# MAGIC         COUNT(*) AS segment_rows,
# MAGIC
# MAGIC         -- Quantidade de combinações diferentes.
# MAGIC         COUNT(
# MAGIC             DISTINCT CONCAT_WS(
# MAGIC                 '||',
# MAGIC                 state,
# MAGIC                 customer_age_group
# MAGIC             )
# MAGIC         ) AS distinct_segments,
# MAGIC
# MAGIC         -- Quantidade de posições diferentes no ranking.
# MAGIC         COUNT(
# MAGIC             DISTINCT revenue_rank
# MAGIC         ) AS distinct_ranks,
# MAGIC
# MAGIC         -- Chaves de segmento nulas.
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN state IS NULL
# MAGIC                   OR customer_age_group IS NULL
# MAGIC                 THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS null_segment_keys,
# MAGIC
# MAGIC         -- Receitas negativas.
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN recognized_revenue < 0 THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS negative_revenue_rows,
# MAGIC
# MAGIC         -- Verifica se a soma dos status corresponde ao total.
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN total_orders <>
# MAGIC                      delivered_orders
# MAGIC                      + shipped_orders
# MAGIC                      + processing_orders
# MAGIC                      + cancelled_orders
# MAGIC                      + returned_orders
# MAGIC                 THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS rows_with_status_difference,
# MAGIC
# MAGIC         -- Verifica se os percentuais estão entre zero e cem.
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN delivery_rate_percentage NOT BETWEEN 0 AND 100
# MAGIC                   OR cancellation_rate_percentage NOT BETWEEN 0 AND 100
# MAGIC                   OR return_rate_percentage NOT BETWEEN 0 AND 100
# MAGIC                   OR coupon_usage_rate_percentage NOT BETWEEN 0 AND 100
# MAGIC                   OR revenue_share_percentage NOT BETWEEN 0 AND 100
# MAGIC                 THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS invalid_rate_rows,
# MAGIC
# MAGIC         -- Verifica se a ação possui um dos três valores definidos.
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN recommended_action NOT IN (
# MAGIC                     'PRIORIZAR_CAMPANHAS',
# MAGIC                     'REVISAR_EXPERIENCIA',
# MAGIC                     'MONITORAR'
# MAGIC                 )
# MAGIC                 THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS invalid_action_rows,
# MAGIC
# MAGIC         -- Soma da participação de todos os segmentos.
# MAGIC         ROUND(
# MAGIC             SUM(revenue_share_percentage),
# MAGIC             2
# MAGIC         ) AS revenue_share_total
# MAGIC
# MAGIC     FROM rower_bootcamp.gold.customer_segment_performance
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     *,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN segment_rows > 0
# MAGIC          AND segment_rows = distinct_segments
# MAGIC          AND segment_rows = distinct_ranks
# MAGIC          AND null_segment_keys = 0
# MAGIC          AND negative_revenue_rows = 0
# MAGIC          AND rows_with_status_difference = 0
# MAGIC          AND invalid_rate_rows = 0
# MAGIC          AND invalid_action_rows = 0
# MAGIC          AND ABS(revenue_share_total - 100) <= 0.20
# MAGIC         THEN 'APROVADO'
# MAGIC         ELSE 'VERIFICAR'
# MAGIC     END AS bonus_gold_quality_status
# MAGIC
# MAGIC FROM segment_quality;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconciliação com a Silver
# MAGIC
# MAGIC Receita, unidades e pedidos entregues da nova Gold devem ser iguais aos totais da Silver para o mesmo conceito de negócio.

# COMMAND ----------

# DBTITLE 1,Calcular métricas agregadas de vendas entregues no silv ...
# MAGIC %sql
# MAGIC WITH silver_totals AS (
# MAGIC     SELECT
# MAGIC         ROUND(COALESCE(SUM(total_amount), 0), 2) AS revenue,
# MAGIC         COALESCE(SUM(quantity), 0) AS units,
# MAGIC         COUNT(DISTINCT order_id) AS delivered_orders
# MAGIC     FROM rower_bootcamp.silver.sales
# MAGIC     WHERE order_status = 'DELIVERED'
# MAGIC )
# MAGIC SELECT * FROM silver_totals;

# COMMAND ----------

# DBTITLE 1,Calcular totais agregados de receita unidades e pedidos
# MAGIC %sql
# MAGIC WITH silver_totals AS (
# MAGIC     SELECT
# MAGIC         ROUND(COALESCE(SUM(total_amount), 0), 2) AS revenue,
# MAGIC         COALESCE(SUM(quantity), 0) AS units,
# MAGIC         COUNT(DISTINCT order_id) AS delivered_orders
# MAGIC     FROM rower_bootcamp.silver.sales
# MAGIC     WHERE order_status = 'DELIVERED'
# MAGIC ),
# MAGIC bonus_gold_totals AS (
# MAGIC     SELECT
# MAGIC         ROUND(COALESCE(SUM(recognized_revenue), 0), 2) AS revenue,
# MAGIC         COALESCE(SUM(delivered_units), 0) AS units,
# MAGIC         COALESCE(SUM(delivered_orders), 0) AS delivered_orders
# MAGIC     FROM rower_bootcamp.gold.customer_segment_performance
# MAGIC )
# MAGIC SELECT
# MAGIC     s.revenue AS silver_revenue,
# MAGIC     g.revenue AS bonus_gold_revenue,
# MAGIC     s.units AS silver_units,
# MAGIC     g.units AS bonus_gold_units,
# MAGIC     s.delivered_orders AS silver_delivered_orders,
# MAGIC     g.delivered_orders AS bonus_gold_delivered_orders,
# MAGIC     ROUND(ABS(s.revenue - g.revenue), 2) AS revenue_difference,
# MAGIC     ABS(s.units - g.units) AS units_difference,
# MAGIC     ABS(s.delivered_orders - g.delivered_orders) AS delivered_orders_difference
# MAGIC FROM silver_totals s
# MAGIC CROSS JOIN bonus_gold_totals g;

# COMMAND ----------

# DBTITLE 1,Definir status de reconciliação entre silver e gold tot ...
# MAGIC %sql
# MAGIC
# MAGIC WITH silver_totals AS (
# MAGIC     SELECT
# MAGIC         ROUND(COALESCE(SUM(total_amount), 0), 2) AS revenue,
# MAGIC         COALESCE(SUM(quantity), 0) AS units,
# MAGIC         COUNT(DISTINCT order_id) AS delivered_orders
# MAGIC     FROM rower_bootcamp.silver.sales
# MAGIC     WHERE order_status = 'DELIVERED'
# MAGIC ),
# MAGIC bonus_gold_totals AS (
# MAGIC     SELECT
# MAGIC         ROUND(COALESCE(SUM(recognized_revenue), 0), 2) AS revenue,
# MAGIC         COALESCE(SUM(delivered_units), 0) AS units,
# MAGIC         COALESCE(SUM(delivered_orders), 0) AS delivered_orders
# MAGIC     FROM rower_bootcamp.gold.customer_segment_performance
# MAGIC )
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN ABS(s.revenue - g.revenue) <= 0.01
# MAGIC          AND s.units = g.units
# MAGIC          AND s.delivered_orders = g.delivered_orders
# MAGIC         THEN 'APROVADO'
# MAGIC         ELSE 'VERIFICAR'
# MAGIC     END AS reconciliation_status
# MAGIC FROM silver_totals s
# MAGIC CROSS JOIN bonus_gold_totals g;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consulta para a decisão de negócio
# MAGIC
# MAGIC A consulta classifica os quinze segmentos de maior receita:
# MAGIC
# MAGIC * `PRIORIZAR_CAMPANHAS`: segmento relevante com baixa perda operacional;
# MAGIC * `REVISAR_EXPERIENCIA`: segmento relevante com cancelamento e devolução elevados;
# MAGIC * `MONITORAR`: demais segmentos.
# MAGIC
# MAGIC O limite de 10% é um parâmetro inicial e deve ser ajustado às metas da empresa.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Analisar métricas de segmentação e desempenho de pedido ...
# MAGIC %sql
# MAGIC WITH order_level AS (
# MAGIC     SELECT
# MAGIC         order_id,
# MAGIC         COALESCE(state, 'NAO_INFORMADO') AS state,
# MAGIC         COALESCE(customer_age_group, 'NAO_INFORMADO') AS customer_age_group,
# MAGIC         MAX(order_status) AS order_status,
# MAGIC         SUM(quantity) AS order_units,
# MAGIC         ROUND(SUM(total_amount), 2) AS order_total_amount,
# MAGIC         ROUND(SUM(COALESCE(coupon_discount, 0)), 2) AS order_coupon_discount,
# MAGIC         MAX(
# MAGIC             CASE
# MAGIC                 WHEN coupon_code IS NOT NULL
# MAGIC                   OR COALESCE(coupon_discount, 0) > 0
# MAGIC                 THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS used_coupon,
# MAGIC         ROUND(AVG(rating), 2) AS order_rating,
# MAGIC         MAX(fulfillment_days) AS fulfillment_days
# MAGIC     FROM rower_bootcamp.silver.sales
# MAGIC     GROUP BY
# MAGIC         order_id,
# MAGIC         COALESCE(state, 'NAO_INFORMADO'),
# MAGIC         COALESCE(customer_age_group, 'NAO_INFORMADO')
# MAGIC ),
# MAGIC segment_metrics AS (
# MAGIC     SELECT
# MAGIC         state,
# MAGIC         customer_age_group,
# MAGIC         COUNT(*) AS total_orders,
# MAGIC         SUM(CASE WHEN order_status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders,
# MAGIC         SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_orders,
# MAGIC         SUM(CASE WHEN order_status = 'RETURNED' THEN 1 ELSE 0 END) AS returned_orders,
# MAGIC         SUM(CASE WHEN order_status = 'DELIVERED' THEN order_units ELSE 0 END) AS delivered_units,
# MAGIC         ROUND(SUM(CASE WHEN order_status = 'DELIVERED' THEN order_total_amount ELSE 0 END), 2) AS recognized_revenue,
# MAGIC         ROUND(AVG(CASE WHEN order_status = 'DELIVERED' THEN order_total_amount END), 2) AS average_delivered_order_value,
# MAGIC         ROUND(AVG(CASE WHEN order_status = 'DELIVERED' THEN fulfillment_days END), 2) AS average_fulfillment_days,
# MAGIC         SUM(used_coupon) AS coupon_orders
# MAGIC     FROM order_level
# MAGIC     GROUP BY state, customer_age_group
# MAGIC ),
# MAGIC calculated_metrics AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROUND(cancelled_orders / NULLIF(total_orders, 0) * 100, 2) AS cancellation_rate_percentage,
# MAGIC         ROUND(returned_orders / NULLIF(total_orders, 0) * 100, 2) AS return_rate_percentage,
# MAGIC         ROUND(coupon_orders / NULLIF(total_orders, 0) * 100, 2) AS coupon_usage_rate_percentage,
# MAGIC         ROUND(
# MAGIC             recognized_revenue /
# MAGIC             NULLIF(SUM(recognized_revenue) OVER (), 0) * 100,
# MAGIC             2
# MAGIC         ) AS revenue_share_percentage,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             ORDER BY recognized_revenue DESC, state ASC, customer_age_group ASC
# MAGIC         ) AS revenue_rank
# MAGIC     FROM segment_metrics
# MAGIC )
# MAGIC SELECT
# MAGIC     revenue_rank,
# MAGIC     state,
# MAGIC     customer_age_group,
# MAGIC     total_orders,
# MAGIC     delivered_orders,
# MAGIC     recognized_revenue,
# MAGIC     revenue_share_percentage,
# MAGIC     average_delivered_order_value,
# MAGIC     coupon_usage_rate_percentage,
# MAGIC     cancellation_rate_percentage,
# MAGIC     return_rate_percentage,
# MAGIC     average_fulfillment_days,
# MAGIC     CASE
# MAGIC         WHEN revenue_share_percentage >= 10
# MAGIC          AND cancellation_rate_percentage <= 5
# MAGIC          AND return_rate_percentage <= 5
# MAGIC         THEN 'PRIORIZAR_CAMPANHAS'
# MAGIC         WHEN revenue_share_percentage >= 10
# MAGIC          AND (cancellation_rate_percentage > 5
# MAGIC               OR return_rate_percentage > 5)
# MAGIC         THEN 'REVISAR_EXPERIENCIA'
# MAGIC         ELSE 'MONITORAR'
# MAGIC     END AS segment_action
# MAGIC FROM calculated_metrics
# MAGIC ORDER BY revenue_rank
# MAGIC LIMIT 15;

# COMMAND ----------

# DBTITLE 1,Definir condições para priorizar campanhas de vendas
# MAGIC %sql
# MAGIC WITH order_level AS (
# MAGIC     SELECT
# MAGIC         order_id,
# MAGIC         COALESCE(state, 'NAO_INFORMADO') AS state,
# MAGIC         COALESCE(customer_age_group, 'NAO_INFORMADO') AS customer_age_group,
# MAGIC         MAX(order_status) AS order_status,
# MAGIC         SUM(quantity) AS order_units,
# MAGIC         ROUND(SUM(total_amount), 2) AS order_total_amount,
# MAGIC         ROUND(SUM(COALESCE(coupon_discount, 0)), 2) AS order_coupon_discount,
# MAGIC         MAX(
# MAGIC             CASE
# MAGIC                 WHEN coupon_code IS NOT NULL
# MAGIC                   OR COALESCE(coupon_discount, 0) > 0
# MAGIC                 THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS used_coupon,
# MAGIC         ROUND(AVG(rating), 2) AS order_rating,
# MAGIC         MAX(fulfillment_days) AS fulfillment_days
# MAGIC     FROM rower_bootcamp.silver.sales
# MAGIC     GROUP BY
# MAGIC         order_id,
# MAGIC         COALESCE(state, 'NAO_INFORMADO'),
# MAGIC         COALESCE(customer_age_group, 'NAO_INFORMADO')
# MAGIC ),
# MAGIC segment_metrics AS (
# MAGIC     SELECT
# MAGIC         state,
# MAGIC         customer_age_group,
# MAGIC         COUNT(*) AS total_orders,
# MAGIC         SUM(CASE WHEN order_status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders,
# MAGIC         SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_orders,
# MAGIC         SUM(CASE WHEN order_status = 'RETURNED' THEN 1 ELSE 0 END) AS returned_orders,
# MAGIC         SUM(CASE WHEN order_status = 'DELIVERED' THEN order_units ELSE 0 END) AS delivered_units,
# MAGIC         ROUND(SUM(CASE WHEN order_status = 'DELIVERED' THEN order_total_amount ELSE 0 END), 2) AS recognized_revenue,
# MAGIC         ROUND(AVG(CASE WHEN order_status = 'DELIVERED' THEN order_total_amount END), 2) AS average_delivered_order_value,
# MAGIC         ROUND(AVG(CASE WHEN order_status = 'DELIVERED' THEN fulfillment_days END), 2) AS average_fulfillment_days,
# MAGIC         SUM(used_coupon) AS coupon_orders
# MAGIC     FROM order_level
# MAGIC     GROUP BY state, customer_age_group
# MAGIC ),
# MAGIC calculated_metrics AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROUND(cancelled_orders / NULLIF(total_orders, 0) * 100, 2) AS cancellation_rate_percentage,
# MAGIC         ROUND(returned_orders / NULLIF(total_orders, 0) * 100, 2) AS return_rate_percentage,
# MAGIC         ROUND(coupon_orders / NULLIF(total_orders, 0) * 100, 2) AS coupon_usage_rate_percentage,
# MAGIC         ROUND(
# MAGIC             recognized_revenue /
# MAGIC             NULLIF(SUM(recognized_revenue) OVER (), 0) * 100,
# MAGIC             2
# MAGIC         ) AS revenue_share_percentage,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             ORDER BY recognized_revenue DESC, state ASC, customer_age_group ASC
# MAGIC         ) AS revenue_rank
# MAGIC     FROM segment_metrics
# MAGIC )
# MAGIC SELECT
# MAGIC     revenue_rank,
# MAGIC     state,
# MAGIC     customer_age_group,
# MAGIC     total_orders,
# MAGIC     delivered_orders,
# MAGIC     recognized_revenue,
# MAGIC     revenue_share_percentage,
# MAGIC     average_delivered_order_value,
# MAGIC     coupon_usage_rate_percentage,
# MAGIC     cancellation_rate_percentage,
# MAGIC     return_rate_percentage,
# MAGIC     average_fulfillment_days,
# MAGIC     CASE
# MAGIC         WHEN revenue_rank <= 15
# MAGIC          AND cancellation_rate_percentage + return_rate_percentage <= 10
# MAGIC         THEN 'PRIORIZAR_CAMPANHAS'
# MAGIC         WHEN revenue_rank <= 15
# MAGIC          AND cancellation_rate_percentage + return_rate_percentage > 10
# MAGIC         THEN 'REVISAR_EXPERIENCIA'
# MAGIC         ELSE 'MONITORAR'
# MAGIC     END AS segment_action
# MAGIC FROM calculated_metrics
# MAGIC ORDER BY revenue_rank
# MAGIC LIMIT 15;

# COMMAND ----------

# DBTITLE 1,Analisar ações recomendadas com base em desempenho fina ...
# MAGIC %sql
# MAGIC WITH order_level AS (
# MAGIC     SELECT
# MAGIC         order_id,
# MAGIC         COALESCE(state, 'NAO_INFORMADO') AS state,
# MAGIC         COALESCE(customer_age_group, 'NAO_INFORMADO') AS customer_age_group,
# MAGIC         MAX(order_status) AS order_status,
# MAGIC         SUM(quantity) AS order_units,
# MAGIC         ROUND(SUM(total_amount), 2) AS order_total_amount,
# MAGIC         ROUND(SUM(COALESCE(coupon_discount, 0)), 2) AS order_coupon_discount,
# MAGIC         MAX(
# MAGIC             CASE
# MAGIC                 WHEN coupon_code IS NOT NULL
# MAGIC                   OR COALESCE(coupon_discount, 0) > 0
# MAGIC                 THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS used_coupon,
# MAGIC         ROUND(AVG(rating), 2) AS order_rating,
# MAGIC         MAX(fulfillment_days) AS fulfillment_days
# MAGIC     FROM rower_bootcamp.silver.sales
# MAGIC     GROUP BY
# MAGIC         order_id,
# MAGIC         COALESCE(state, 'NAO_INFORMADO'),
# MAGIC         COALESCE(customer_age_group, 'NAO_INFORMADO')
# MAGIC ),
# MAGIC segment_metrics AS (
# MAGIC     SELECT
# MAGIC         state,
# MAGIC         customer_age_group,
# MAGIC         COUNT(*) AS total_orders,
# MAGIC         SUM(CASE WHEN order_status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders,
# MAGIC         SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_orders,
# MAGIC         SUM(CASE WHEN order_status = 'RETURNED' THEN 1 ELSE 0 END) AS returned_orders,
# MAGIC         SUM(CASE WHEN order_status = 'DELIVERED' THEN order_units ELSE 0 END) AS delivered_units,
# MAGIC         ROUND(SUM(CASE WHEN order_status = 'DELIVERED' THEN order_total_amount ELSE 0 END), 2) AS recognized_revenue,
# MAGIC         ROUND(AVG(CASE WHEN order_status = 'DELIVERED' THEN order_total_amount END), 2) AS average_delivered_order_value,
# MAGIC         ROUND(AVG(CASE WHEN order_status = 'DELIVERED' THEN fulfillment_days END), 2) AS average_fulfillment_days,
# MAGIC         SUM(used_coupon) AS coupon_orders
# MAGIC     FROM order_level
# MAGIC     GROUP BY state, customer_age_group
# MAGIC ),
# MAGIC calculated_metrics AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROUND(cancelled_orders / NULLIF(total_orders, 0) * 100, 2) AS cancellation_rate_percentage,
# MAGIC         ROUND(returned_orders / NULLIF(total_orders, 0) * 100, 2) AS return_rate_percentage,
# MAGIC         ROUND(coupon_orders / NULLIF(total_orders, 0) * 100, 2) AS coupon_usage_rate_percentage,
# MAGIC         ROUND(
# MAGIC             recognized_revenue /
# MAGIC             NULLIF(SUM(recognized_revenue) OVER (), 0) * 100,
# MAGIC             2
# MAGIC         ) AS revenue_share_percentage,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             ORDER BY recognized_revenue DESC, state ASC, customer_age_group ASC
# MAGIC         ) AS revenue_rank
# MAGIC     FROM segment_metrics
# MAGIC )
# MAGIC SELECT
# MAGIC     revenue_rank,
# MAGIC     state,
# MAGIC     customer_age_group,
# MAGIC     total_orders,
# MAGIC     delivered_orders,
# MAGIC     recognized_revenue,
# MAGIC     revenue_share_percentage,
# MAGIC     average_delivered_order_value,
# MAGIC     coupon_usage_rate_percentage,
# MAGIC     cancellation_rate_percentage,
# MAGIC     return_rate_percentage,
# MAGIC     average_fulfillment_days,
# MAGIC     CASE
# MAGIC         WHEN revenue_rank <= 15
# MAGIC          AND cancellation_rate_percentage + return_rate_percentage <= 10
# MAGIC         THEN 'PRIORIZAR_CAMPANHAS'
# MAGIC         WHEN revenue_rank <= 15
# MAGIC          AND cancellation_rate_percentage + return_rate_percentage > 10
# MAGIC         THEN 'REVISAR_EXPERIENCIA'
# MAGIC         ELSE 'MONITORAR'
# MAGIC     END AS recommended_action
# MAGIC FROM calculated_metrics
# MAGIC ORDER BY revenue_rank
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Achado adicional da revisão
# MAGIC
# MAGIC A fonte contém o status SHIPPED. A validação atual da Silver não inclui esse valor na lista de estados conhecidos e, por isso, retorna VERIFICAR para 12.459 registros.
# MAGIC
# MAGIC A tabela bônus já inclui a coluna shipped_orders. Na validação silver_quality do projeto original, inclua SHIPPED na lista de estados aceitos.

# COMMAND ----------

# DBTITLE 1,Analisar total de pedidos por status de entrega
# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_status,
# MAGIC     COUNT(DISTINCT order_id) AS total_orders
# MAGIC FROM rower_bootcamp.silver.sales
# MAGIC GROUP BY order_status
# MAGIC ORDER BY total_orders DESC, order_status;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Ajuste da validação de status
# MAGIC
# MAGIC Na consulta silver_quality, utilize:
# MAGIC ```sql
# MAGIC order_status NOT IN (
# MAGIC     'DELIVERED',
# MAGIC     'SHIPPED',
# MAGIC     'CANCELLED',
# MAGIC     'RETURNED',
# MAGIC     'PROCESSING'
# MAGIC )
# MAGIC Esse ajuste não modifica os dados. Ele alinha a validação aos valores existentes na fonte.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Criação da view governada

# COMMAND ----------

# DBTITLE 1,Criar view para as 10 principais marcas por receita
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW rower_bootcamp.gold.vw_top_10_brands
# MAGIC (
# MAGIC     revenue_rank COMMENT 'Posição da marca no ranking global de receita reconhecida',
# MAGIC     category COMMENT 'Categoria do produto',
# MAGIC     brand COMMENT 'Marca do produto',
# MAGIC     delivered_orders COMMENT 'Pedidos entregues associados à marca',
# MAGIC     delivered_units COMMENT 'Unidades entregues',
# MAGIC     recognized_revenue COMMENT 'Receita dos pedidos entregues',
# MAGIC     revenue_share_percentage COMMENT 'Participação percentual na receita entregue'
# MAGIC )
# MAGIC COMMENT 'View governada com as dez marcas de maior receita reconhecida'
# MAGIC AS
# MAGIC SELECT
# MAGIC     revenue_rank,
# MAGIC     category,
# MAGIC     brand,
# MAGIC     delivered_orders,
# MAGIC     delivered_units,
# MAGIC     recognized_revenue,
# MAGIC     revenue_share_percentage
# MAGIC FROM rower_bootcamp.gold.brand_sales_ranking
# MAGIC WHERE revenue_rank <= 10;

# COMMAND ----------

# DBTITLE 1,Analisar marcas principais por receita e categoria
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM rower_bootcamp.gold.vw_top_10_brands
# MAGIC ORDER BY revenue_rank, category, brand;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Permissões e governança
# MAGIC
# MAGIC A execução do notebook **não concede permissões**. Um administrador deve conferir o grupo de consumidores autorizado e executar manualmente os comandos de `admin/grant_view.sql`, substituindo o nome ilustrativo do grupo. A consulta de auditoria `SHOW GRANTS` deve permanecer no ambiente privado, pois pode revelar identificadores de usuários.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Reconciliação Silver × Gold
# MAGIC
# MAGIC Esta reconciliação usa o mesmo conceito em todas as camadas: somente pedidos entregues. A tolerância monetária é de um centavo.

# COMMAND ----------

# DBTITLE 1,Comparar totais e diferenças entre vendas silver e gold
# MAGIC %sql
# MAGIC WITH silver_totals AS (
# MAGIC     SELECT
# MAGIC         -- Total geral de pedidos.
# MAGIC         COUNT(DISTINCT order_id) AS total_orders,
# MAGIC
# MAGIC         -- Total de pedidos por status.
# MAGIC         COUNT(
# MAGIC             DISTINCT CASE
# MAGIC                 WHEN order_status = 'DELIVERED' THEN order_id
# MAGIC             END
# MAGIC         ) AS delivered_orders,
# MAGIC
# MAGIC         COUNT(
# MAGIC             DISTINCT CASE
# MAGIC                 WHEN order_status = 'SHIPPED' THEN order_id
# MAGIC             END
# MAGIC         ) AS shipped_orders,
# MAGIC
# MAGIC         COUNT(
# MAGIC             DISTINCT CASE
# MAGIC                 WHEN order_status = 'PROCESSING' THEN order_id
# MAGIC             END
# MAGIC         ) AS processing_orders,
# MAGIC
# MAGIC         COUNT(
# MAGIC             DISTINCT CASE
# MAGIC                 WHEN order_status = 'CANCELLED' THEN order_id
# MAGIC             END
# MAGIC         ) AS cancelled_orders,
# MAGIC
# MAGIC         COUNT(
# MAGIC             DISTINCT CASE
# MAGIC                 WHEN order_status = 'RETURNED' THEN order_id
# MAGIC             END
# MAGIC         ) AS returned_orders,
# MAGIC
# MAGIC         -- Unidades e receita dos pedidos entregues.
# MAGIC         COALESCE(
# MAGIC             SUM(
# MAGIC                 CASE
# MAGIC                     WHEN order_status = 'DELIVERED'
# MAGIC                     THEN quantity
# MAGIC                     ELSE 0
# MAGIC                 END
# MAGIC             ),
# MAGIC             0
# MAGIC         ) AS delivered_units,
# MAGIC
# MAGIC         ROUND(
# MAGIC             COALESCE(
# MAGIC                 SUM(
# MAGIC                     CASE
# MAGIC                         WHEN order_status = 'DELIVERED'
# MAGIC                         THEN total_amount
# MAGIC                         ELSE 0
# MAGIC                     END
# MAGIC                 ),
# MAGIC                 0
# MAGIC             ),
# MAGIC             2
# MAGIC         ) AS recognized_revenue
# MAGIC
# MAGIC     FROM rower_bootcamp.silver.sales
# MAGIC ),
# MAGIC
# MAGIC gold_totals AS (
# MAGIC     SELECT
# MAGIC         COALESCE(SUM(total_orders), 0) AS total_orders,
# MAGIC         COALESCE(SUM(delivered_orders), 0) AS delivered_orders,
# MAGIC         COALESCE(SUM(shipped_orders), 0) AS shipped_orders,
# MAGIC         COALESCE(SUM(processing_orders), 0) AS processing_orders,
# MAGIC         COALESCE(SUM(cancelled_orders), 0) AS cancelled_orders,
# MAGIC         COALESCE(SUM(returned_orders), 0) AS returned_orders,
# MAGIC         COALESCE(SUM(delivered_units), 0) AS delivered_units,
# MAGIC
# MAGIC         ROUND(
# MAGIC             COALESCE(SUM(recognized_revenue), 0),
# MAGIC             2
# MAGIC         ) AS recognized_revenue
# MAGIC
# MAGIC     FROM rower_bootcamp.gold.customer_segment_performance
# MAGIC ),
# MAGIC
# MAGIC differences AS (
# MAGIC     SELECT
# MAGIC         ABS(s.total_orders - g.total_orders)
# MAGIC             AS total_orders_difference,
# MAGIC
# MAGIC         ABS(s.delivered_orders - g.delivered_orders)
# MAGIC             AS delivered_orders_difference,
# MAGIC
# MAGIC         ABS(s.shipped_orders - g.shipped_orders)
# MAGIC             AS shipped_orders_difference,
# MAGIC
# MAGIC         ABS(s.processing_orders - g.processing_orders)
# MAGIC             AS processing_orders_difference,
# MAGIC
# MAGIC         ABS(s.cancelled_orders - g.cancelled_orders)
# MAGIC             AS cancelled_orders_difference,
# MAGIC
# MAGIC         ABS(s.returned_orders - g.returned_orders)
# MAGIC             AS returned_orders_difference,
# MAGIC
# MAGIC         ABS(s.delivered_units - g.delivered_units)
# MAGIC             AS delivered_units_difference,
# MAGIC
# MAGIC         ROUND(
# MAGIC             ABS(s.recognized_revenue - g.recognized_revenue),
# MAGIC             2
# MAGIC         ) AS revenue_difference
# MAGIC
# MAGIC     FROM silver_totals s
# MAGIC     CROSS JOIN gold_totals g
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     *,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN total_orders_difference = 0
# MAGIC          AND delivered_orders_difference = 0
# MAGIC          AND shipped_orders_difference = 0
# MAGIC          AND processing_orders_difference = 0
# MAGIC          AND cancelled_orders_difference = 0
# MAGIC          AND returned_orders_difference = 0
# MAGIC          AND delivered_units_difference = 0
# MAGIC          AND revenue_difference <= 0.01
# MAGIC         THEN 'APROVADO'
# MAGIC         ELSE 'VERIFICAR'
# MAGIC     END AS reconciliation_status
# MAGIC
# MAGIC FROM differences;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. Evidências do Delta Lake
# MAGIC
# MAGIC Cada evidência fica em uma célula própria, porque o notebook exibe apenas o resultado da última instrução de uma célula SQL com vários comandos.

# COMMAND ----------

# DBTITLE 1,Analisar a estrutura da tabela orders_raw no bronze
# MAGIC %sql
# MAGIC DESCRIBE DETAIL rower_bootcamp.bronze.orders_raw;

# COMMAND ----------

# DBTITLE 1,Analisar a estrutura da tabela de vendas no silver laye ...
# MAGIC %sql
# MAGIC DESCRIBE DETAIL rower_bootcamp.silver.sales;

# COMMAND ----------

# DBTITLE 1,Analisar estrutura da tabela monthly_sales_indicators
# MAGIC %sql
# MAGIC DESCRIBE DETAIL rower_bootcamp.gold.monthly_sales_indicators;

# COMMAND ----------

# DBTITLE 1,Analisar a estrutura da tabela silver.sales no catálogo
# MAGIC %sql
# MAGIC DESCRIBE HISTORY rower_bootcamp.silver.sales LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 18. Explicação do reprocessamento
# MAGIC
# MAGIC - A Bronze é incremental: Auto Loader registra os arquivos processados no checkpoint e, ao executar novamente, lê apenas arquivos novos. Ele preserva as linhas brutas e os metadados da origem.
# MAGIC - Silver e Gold usam `CREATE OR REPLACE TABLE`, apropriado para este dataset pequeno. A substituição é atômica, mantém o histórico Delta e permite reproduzir integralmente as regras atuais.
# MAGIC - Após cada recriação, os comentários de colunas são reaplicados para impedir que a documentação fique desatualizada.
# MAGIC - Para testar idempotência, anote as contagens da Bronze, execute novamente a célula do Auto Loader e confirme que as contagens não aumentaram.
# MAGIC - Para testar incremento, adicione um novo arquivo com nome `sales_*.csv` ou `products_*.csv`, execute a Bronze e confirme que somente o novo arquivo foi ingerido. Em seguida, reexecute Silver, Gold, view e reconciliação.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 19. Conclusão
# MAGIC
# MAGIC A solução final implementa a arquitetura Medalhão, mantém rastreabilidade dos arquivos, utiliza ingestão incremental com Auto Loader, separa registros inválidos em uma tabela de quarentena e evita perdas silenciosas durante a integração com produtos.
# MAGIC
# MAGIC O projeto entrega três tabelas Gold: indicadores mensais, ranking de marcas e desempenho por segmento regional e etário. Também disponibiliza uma view governada, documenta a concessão manual de permissões mínimas e reconcilia os resultados analíticos com a camada Silver.
# MAGIC
# MAGIC As validações verificam idempotência, duplicidade, qualidade dos status, consistência financeira, granularidade, percentuais, participação na receita e igualdade dos totais entre Silver e Gold.
# MAGIC
# MAGIC Antes da entrega:
# MAGIC
# MAGIC execute o notebook completo, do início ao fim;
# MAGIC confirme que todas as validações apresentam APROVADO;
# MAGIC verifique se todas as diferenças das reconciliações são iguais a zero;
# MAGIC confira as três tabelas Gold no Catalog Explorer;
# MAGIC exporte para o GitHub apenas o código-fonte sem os resultados das células; mantenha dados, caminhos internos e evidências com usuários em ambiente privado.
