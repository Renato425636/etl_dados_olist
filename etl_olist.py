import logging
import sys
import os
import requests
import zipfile
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, monotonically_increasing_id, to_date, year, month, dayofmonth, quarter, dayofweek, date_format, udf, count, lit, avg, stddev, min, max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, FloatType
from typing import Dict, List

class AdvancedStarSchemaPipeline:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.spark = self._initialize_spark()
        self.source_tables: Dict[str, DataFrame] = {}
        self.dimensional_models: Dict[str, DataFrame] = {}
        self._setup_logging()

    def _load_config(self, config_path: str) -> Dict:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def _setup_logging(self):
        logging.basicConfig(level=self.config["log_level"], format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", stream=sys.stdout)
        self.logger = logging.getLogger(self.config["pipeline_name"])
        logging.getLogger("py4j").setLevel(logging.WARNING)
        logging.getLogger("pyspark").setLevel(logging.WARNING)

    def _initialize_spark(self) -> SparkSession:
        try:
            return (
                SparkSession.builder
                .appName(self.config["spark"]["app_name"])
                .master(self.config["spark"]["master"])
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .getOrCreate()
            )
        except Exception as e:
            logging.critical(f"Falha ao inicializar SparkSession: {e}", exc_info=True)
            sys.exit(1)
    
    def _download_and_unzip_data(self):
        source_path = self.config["data"]["source_path"]
        if os.path.exists(source_path) and any(f.endswith('.csv') for f in os.listdir(source_path)):
            self.logger.info("Arquivos de dados já existem. Pulando o download.")
            return

        os.makedirs(source_path, exist_ok=True)
        zip_path = os.path.join(source_path, "olist_dataset.zip")
        try:
            self.logger.info(f"Baixando dados de {self.config['data']['url']}...")
            response = requests.get(self.config["data"]["url"], stream=True, timeout=60)
            response.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192): f.write(chunk)
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref: zip_ref.extractall(source_path)
            os.remove(zip_path)
            self.logger.info("Download e descompactação concluídos.")
        except Exception as e:
            self.logger.critical(f"Falha no download ou descompactação dos dados: {e}", exc_info=True)
            sys.exit(1)

    def _load_source_tables(self, schemas: Dict[str, StructType]):
        self.logger.info("Carregando tabelas de origem...")
        source_path = self.config["data"]["source_path"]
        for name, schema in schemas.items():
            file_path = f"{source_path}/olist_{name.replace('_', '_dataset_')}.csv"
            self.source_tables[name] = self.spark.read.csv(file_path, header=True, schema=schema)

    def _create_dimensions(self):
        self.logger.info("Iniciando a criação das tabelas de dimensão...")
        self.dimensional_models["Dim_Geolocalizacao"] = self._create_dim_geolocalizacao()
        self.dimensional_models["Dim_Cliente"] = self._create_dim_cliente()
        self.dimensional_models["Dim_Produto"] = self._create_dim_produto()
        self.dimensional_models["Dim_Vendedor"] = self._create_dim_vendedor()
        self.dimensional_models["Dim_Tempo"] = self._create_dim_tempo()
    
    def _create_dim_geolocalizacao(self) -> DataFrame:
        df_geo = self.source_tables["geolocation"].groupBy("geolocation_zip_code_prefix").agg(
            avg("geolocation_lat").alias("latitude"),
            avg("geolocation_lng").alias("longitude")
        )
        return df_geo.withColumn("id_geolocalizacao", monotonically_increasing_id())

    def _create_dim_cliente(self) -> DataFrame:
        df_customers = self.source_tables["customers"]
        df_geo = self.dimensional_models["Dim_Geolocalizacao"]
        dim_cliente = df_customers.join(df_geo, df_customers.customer_zip_code_prefix == df_geo.geolocation_zip_code_prefix, "left") \
            .select(
                col("customer_unique_id").alias("id_negocio_cliente"),
                col("customer_city").alias("cidade_cliente"),
                col("customer_state").alias("estado_cliente"),
                col("id_geolocalizacao")
            ).distinct()
        return dim_cliente.withColumn("id_cliente", monotonically_increasing_id())

    def _create_dim_produto(self) -> DataFrame:
        df_products = self.source_tables["products"]
        df_translation = self.source_tables["translation"]
        capitalize_udf = udf(lambda s: s.replace("_", " ").title() if s else "N/A", StringType())
        dim_produto = df_products.join(df_translation, "product_category_name", "left").select(
            col("product_id").alias("id_negocio_produto"),
            capitalize_udf(col("product_category_name_english")).alias("categoria_produto"),
            col("product_photos_qty")
        ).distinct()
        return dim_produto.withColumn("id_produto", monotonically_increasing_id())

    def _create_dim_vendedor(self) -> DataFrame:
        df_sellers = self.source_tables["sellers"]
        df_geo = self.dimensional_models["Dim_Geolocalizacao"]
        dim_vendedor = df_sellers.join(df_geo, df_sellers.seller_zip_code_prefix == df_geo.geolocation_zip_code_prefix, "left") \
            .select(
                col("seller_id").alias("id_negocio_vendedor"),
                col("seller_city").alias("cidade_vendedor"),
                col("seller_state").alias("estado_vendedor"),
                col("id_geolocalizacao")
            ).distinct()
        return dim_vendedor.withColumn("id_vendedor", monotonically_increasing_id())

    def _create_dim_tempo(self) -> DataFrame:
        df_orders = self.source_tables["orders"]
        date_df = df_orders.select(to_date(col("order_purchase_timestamp")).alias("data")).distinct().na.drop()
        dim_tempo = date_df.select(
            col("data"), year("data").alias("ano"), month("data").alias("mes"), dayofmonth("data").alias("dia"),
            quarter("data").alias("trimestre"), date_format("data", "E").alias("nome_dia_semana")
        )
        return dim_tempo.withColumn("id_tempo", monotonically_increasing_id())

    def _create_fact_table(self):
        self.logger.info("Iniciando a criação da tabela Fato_Vendas...")
        df_items = self.source_tables["order_items"]
        df_orders = self.source_tables["orders"]
        df_customers = self.source_tables["customers"]

        base_fato = df_items.join(df_orders, "order_id", "inner").join(df_customers.select("customer_id", "customer_unique_id"), "customer_id", "inner")
        
        fato_com_chaves = base_fato \
            .join(self.dimensional_models["Dim_Produto"], base_fato.product_id == self.dimensional_models["Dim_Produto"].id_negocio_produto, "left") \
            .join(self.dimensional_models["Dim_Cliente"], base_fato.customer_unique_id == self.dimensional_models["Dim_Cliente"].id_negocio_cliente, "left") \
            .join(self.dimensional_models["Dim_Vendedor"], base_fato.seller_id == self.dimensional_models["Dim_Vendedor"].id_negocio_vendedor, "left") \
            .join(self.dimensional_models["Dim_Tempo"], to_date(base_fato.order_purchase_timestamp) == self.dimensional_models["Dim_Tempo"].data, "left")
        
        self.dimensional_models["Fato_Vendas"] = fato_com_chaves.select(
            col("id_produto"), col("id_cliente"), col("id_vendedor"), col("id_tempo"),
            col("order_id").alias("id_pedido"), col("price").alias("preco"),
            col("freight_value").alias("valor_frete"), col("order_status").alias("status_pedido")
        )

    def _run_data_quality_checks(self):
        self.logger.info("Iniciando verificação de qualidade dos dados (Data Quality Checks)...")
        for name, df in self.dimensional_models.items():
            if name.startswith("Dim_"):
                pk = f"id_{name.split('_')[1].lower()}"
                if df.filter(col(pk).isNull()).count() > 0: raise ValueError(f"DQ FALHOU: {name} contém chaves primárias nulas.")
                if df.count() != df.select(pk).distinct().count(): raise ValueError(f"DQ FALHOU: Chave primária de {name} não é única.")
        
        self._check_referential_integrity("Fato_Vendas", "id_produto", "Dim_Produto", "id_produto")
        self._check_accepted_values("Fato_Vendas", "status_pedido", self.config["data_quality"]["accepted_order_status"])
        self._check_non_negative("Fato_Vendas", "preco")
        self.logger.info("Verificação de qualidade dos dados concluída com sucesso.")

    def _check_referential_integrity(self, fact_name, fk, dim_name, pk):
        fact_df = self.dimensional_models[fact_name]
        dim_df = self.dimensional_models[dim_name]
        bad_records = fact_df.join(dim_df, fact_df[fk] == dim_df[pk], "left_anti").count()
        if bad_records > 0: raise ValueError(f"DQ FALHOU: {bad_records} registros em {fact_name} violam a integridade referencial com {dim_name}.")

    def _check_accepted_values(self, table_name, column_name, accepted_values: List[str]):
        df = self.dimensional_models[table_name]
        bad_records = df.filter(~col(column_name).isin(accepted_values)).count()
        if bad_records > 0: raise ValueError(f"DQ FALHOU: {bad_records} registros em {table_name} têm valores inválidos na coluna {column_name}.")

    def _check_non_negative(self, table_name, column_name):
        df = self.dimensional_models[table_name]
        bad_records = df.filter(col(column_name) < 0).count()
        if bad_records > 0: raise ValueError(f"DQ FALHOU: {bad_records} registros em {table_name} têm valores negativos em {column_name}.")

    def _run_data_profiling(self):
        self.logger.info("Executando perfil de dados (Data Profiling)...")
        profiling_path = self.config["data"]["profiling_path"]
        os.makedirs(profiling_path, exist_ok=True)
        
        fato_df = self.dimensional_models["Fato_Vendas"]
        profile = fato_df.select("preco", "valor_frete").summary("count", "mean", "stddev", "min", "max").toJSON().collect()
        
        with open(os.path.join(profiling_path, "Fato_Vendas_profile.json"), 'w') as f:
            f.write(str(profile))
        self.logger.info("Relatório de perfil de dados salvo.")

    def _save_models(self):
        self.logger.info("Salvando modelos dimensionais em formato Parquet...")
        output_path = self.config["data"]["output_path"]
        for name, df in self.dimensional_models.items():
            df.write.mode("overwrite").parquet(os.path.join(output_path, name))

    def run(self, schemas: Dict[str, StructType]):
        self.logger.info("--- INICIANDO PIPELINE DE CONSTRUÇÃO DO STAR SCHEMA ---")
        try:
            self._download_and_unzip_data()
            self._load_source_tables(schemas)
            self._create_dimensions()
            self._create_fact_table()
            self._run_data_quality_checks()
            self._run_data_profiling()
            self._save_models()
            self.logger.info("--- PIPELINE CONCLUÍDO COM SUCESSO ---")
        except Exception as e:
            self.logger.critical(f"--- FALHA NA EXECUÇÃO DO PIPELINE: {e} ---", exc_info=True)
            sys.exit(1)
        finally:
            self.spark.stop()
            self.logger.info("SparkSession finalizada.")

if __name__ == "__main__":
    CONFIG_FILE = "config.yaml"
    SCHEMAS = {
        "customers": StructType([StructField("customer_id", StringType()), StructField("customer_unique_id", StringType()), StructField("customer_zip_code_prefix", IntegerType()), StructField("customer_city", StringType()), StructField("customer_state", StringType())]),
        "sellers": StructType([StructField("seller_id", StringType()), StructField("seller_zip_code_prefix", IntegerType()), StructField("seller_city", StringType()), StructField("seller_state", StringType())]),
        "products": StructType([StructField("product_id", StringType()), StructField("product_category_name", StringType()), StructField("product_name_lenght", IntegerType()), StructField("product_description_lenght", IntegerType()), StructField("product_photos_qty", IntegerType())]),
        "orders": StructType([StructField("order_id", StringType()), StructField("customer_id", StringType()), StructField("order_status", StringType()), StructField("order_purchase_timestamp", TimestampType())]),
        "order_items": StructType([StructField("order_id", StringType()), StructField("order_item_id", IntegerType()), StructField("product_id", StringType()), StructField("seller_id", StringType()), StructField("price", DoubleType()), StructField("freight_value", DoubleType())]),
        "translation": StructType([StructField("product_category_name", StringType()), StructField("product_category_name_english", StringType())]),
        "geolocation": StructType([StructField("geolocation_zip_code_prefix", IntegerType()), StructField("geolocation_lat", FloatType()), StructField("geolocation_lng", FloatType()), StructField("geolocation_city", StringType()), StructField("geolocation_state", StringType())])
    }

    pipeline = AdvancedStarSchemaPipeline(config_path=CONFIG_FILE)
    pipeline.run(schemas=SCHEMAS)
