import os
import base64
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import expr, lit

os.environ['SPARK_HOME'] = '/data/spark-3.5.1-bin-hadoop3'
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-11.0.13.0.8-4.el8_5.x86_64'
os.environ['SPARK_CONF_DIR'] = '/home/dev/duy/threatq/conf'
os.environ['AWS_ACCESS_KEY_ID'] = base64.b64decode('VmwyMVpEVUVxMVhOaURETg==').decode('utf-8')
os.environ['AWS_SECRET_ACCESS_KEY'] = base64.b64decode('U3U5b21PcHBxUHZsOXE1OUFCZElmendPekRpMVBwZ1o=').decode('utf-8')
os.environ['AWS_REGION'] = base64.b64decode('YXV0bw==').decode('utf-8')
os.environ['AWS_ENDPOINT'] = 'http://192.168.1.151'
os.environ['HIVE_METASTORE_URIS'] = 'thrift://192.168.1.156:9083'
os.environ['HIVE_WAREHOUSE_DIR'] = 's3a://data/warehouse'

spark = SparkSession.builder \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

def csv_to_table(csv_path, table, schema, load_date='2024-11-04'):
    df = spark.read \
        .schema(schema) \
        .option("header", "true") \
        .option("dateFormat", "yyyy-MM-dd") \
        .option("multiLine", "true") \
        .csv(csv_path)

    df = add_system_columns(df, table, load_date)

    # spark.sql(f"drop table if exists {table} purge")
    df.write.format('iceberg').mode('append').saveAsTable(table)

def add_system_columns(df, table, load_date):
    return df \
        .withColumns({
            "optime": expr(f"from_unixtime(unix_timestamp(timestamp'{load_date}') + rand()*86400)"),
        }) \
        .withColumns({
            "ktime": expr("optime"),
            "koffset": expr("row_number() over (partition by 1 order by optime)"),
            "cdc_operation": expr("element_at(array('U','D'), cast( (rand()*2 + 1) as int))"),
            "record_source": lit(table)
        })

def alter_table_add_system_columns(table, load_date):
    exists_query = f"SHOW TABLES IN {table.split('.')[0]} LIKE '{table.split('.')[1]}'"
    if len(spark.sql(exists_query).collect()) > 0:
        spark.sql(f"alter table {table} rename to {table}_bk")
    
    df = spark.table(f"{table}_bk")
    df = add_system_columns(df, table, load_date)
    df.write.format('iceberg').mode('overwrite').saveAsTable(table)
    spark.sql(f"drop table {table}_bk purge")

# csv_to_table(
#     "/home/dev/duy/stb_samples/target/corebank_branch.csv",
#     "source.corebank_branch",
#     "BR_CD string, BR_NM string, BR_ADDR string, PARENT_BR_CD string, PARENT_BR_NM string, PARENT_BR_CD_LV1 string, PARENT_BR_NM_LV1 string, AREA_4_CD string, AREA_4_NM string"
# )

# csv_to_table(
#     "/home/dev/duy/stb_samples/target/corecard_customer.csv",
#     "source.corecard_customer",
#     "CB_CUS_ID bigint, CB_CIF_NO bigint, CB_CUSTOMER_IDNO string, CB_SEX string, CB_DOB date, CB_NATIONALITY string, CB_MOBILE_NO string, CB_EMAIL string, CB_USER2_DATE_1 date, CB_OCCUPN string, CB_CARDHOLDER_NAME string, CB_ID_TYPE string, CB_CREATION_DATE date, BRN_CODE string"
# )

# csv_to_table(
#     "/home/dev/duy/stb_samples/target/corebank_customer.csv",
#     "source.corebank_customer",
#     "CUS_CUSTOMER_CODE bigint, CUS_NAME_1 string, CUS_GENDER string, CUS_BIRTH_INCORP_DATE date, CUS_NATIONALITY string, CUS_STREET string, CUS_ADDRESS string, CUS_MOBILE_NUMBER string, CUS_EMAIL_1 string, CUS_LEGAL_ID string, CUS_LEGAL_DOC_NAME string, CUS_LEGAL_ISS_DATE date, CUS_LEGAL_ISS_AUTH string, CUS_POSITION int, CUS_SECTOR int, CUS_CREATION_DATE date, BRN_CODE string"
# )

# csv_to_table(
#     "/home/dev/duy/stb_samples/target/crm_customer.csv",
#     "source.crm_customer",
#     "CRM_CUS_ID bigint, CRM_CUS_NAME string, CRM_CIF_NO bigint, CRM_CUSTOMER_IDNO string, CRM_CREATION_DATE date, BRN_CODE string"
# )

# csv_to_table(
#     "/home/dev/duy/stb_samples/target/corebank_corp.csv",
#     "source.corebank_corp",
#     "CUS_CUSTOMER_CODE bigint, CUS_NAME_1 string, CUS_ESTAB_CODE string, CUS_ESTAB_CODE_ISS_AUTH string, CUS_ESTAB_CODE_ISS_DATE date, CUS_EMAIL_1 string, CUS_STREET string, CUS_TAX_ID string, CUS_TAX_ID_ISS_AUTH string, CUS_TAX_ID_ISS_DATE date, CUS_RESIDENCE string, CB_MOBILE_NO string, CUS_CREATION_DATE date, BRN_CODE string"
# )

# csv_to_table(
#     "/home/dev/duy/stb_samples/target/corecard_corp.csv",
#     "source.corecard_corp",
#     "CO_CORPORATE_ID bigint, CO_COMPANY_NAME string, CB_CUSTOMER_CODE bigint, CUS_ESTAB_CODE string, CUS_ESTAB_CODE_ISS_AUTH string, CO_REGISTRATION_DATE date, CB_EMAIL string, CO_REG_ADD_LINE1 string, CB_ISS_AUTH string, CB_CUSTOMER_IDNO string, CB_USER2_DATE_1 date, CB_RESIDENCY_CODE string, CB_MOBILE_NO string, CUS_CREATION_DATE date, BRN_CODE string"
# )

# csv_to_table(
#     "/home/dev/duy/stb_samples/target/corecard_customer_2.csv",
#     "source.corecard_customer",
#     "CB_CUS_ID bigint, CB_CIF_NO bigint, CB_CUSTOMER_IDNO string, CB_SEX string, CB_DOB date, CB_NATIONALITY string, CB_MOBILE_NO string, CB_EMAIL string, CB_USER2_DATE_1 date, CB_OCCUPN string, CB_CARDHOLDER_NAME string, CB_ID_TYPE string, CB_CREATION_DATE date, BRN_CODE string",
#     "2024-11-05"
# )

# csv_to_table(
#     "/home/dev/duy/stb_samples/target/corebank_customer_2.csv",
#     "source.corebank_customer",
#     "CUS_CUSTOMER_CODE bigint, CUS_NAME_1 string, CUS_GENDER string, CUS_BIRTH_INCORP_DATE date, CUS_NATIONALITY string, CUS_STREET string, CUS_ADDRESS string, CUS_MOBILE_NUMBER string, CUS_EMAIL_1 string, CUS_LEGAL_ID string, CUS_LEGAL_DOC_NAME string, CUS_LEGAL_ISS_DATE date, CUS_LEGAL_ISS_AUTH string, CUS_POSITION int, CUS_SECTOR int, CUS_CREATION_DATE date, BRN_CODE string",
#     "2024-11-05"
# )

# alter_table_add_system_columns('source.corebank_customer', '2024-11-04')
# alter_table_add_system_columns('source.corecard_customer', '2024-11-04')
# alter_table_add_system_columns('source.corebank_corp', '2024-11-04')
# alter_table_add_system_columns('source.corecard_corp', '2024-11-04')
# alter_table_add_system_columns('source.corebank_branch', '2024-11-04')
# alter_table_add_system_columns('source.crm_customer', '2024-11-04')

# spark.sql("select from_unixtime(unix_timestamp(timestamp'2024-11-04') + rand()*86400)").show()

# spark.sql("select * from source.corebank_customer").show()
spark.sql("select * from integration_demo.vw_ref_eod").show()