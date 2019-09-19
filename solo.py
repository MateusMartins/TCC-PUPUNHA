import sys
sys.dont_write_bytecode=True
from pyspark.sql.functions import expr, col, max as _max, mean as _mean, sum as _sum
from pyspark.sql import SparkSession, Row, DataFrameWriter

spark = SparkSession.builder.appName('solo').config("spark.debug.maxToStringFields", 4000).master("local").enableHiveSupport().getOrCreate()

# Efetua a leitura do csv de clima
print('EFETUANDO LEITURA DO CSV DE CLIMA')
df_solo = spark.read.csv('C:/projeto/datasets/Dataset-Solo.csv', header=True, sep=';')
df_solo.show(10, False)

for name in df_solo.columns:
    df_solo = df_solo.withColumn(name, df_solo[name].cast("Double"))

df_solo = df_solo.withColumn('SB', (col('K') + col('Ca') + col('Mg'))) \
    .withColumn('CTC_efetiva', (col('SB') + col('Al'))) \
    .withColumn('CTC_total', (col('Ca') + col('Mg') + col('K') + col('H+Al'))) \
    .withColumn('V', (100*(col('SB')/col('CTC_total'))))

# Efetuando gravação do dataframe em csv
print('SALVANDO DATAFRAME SOLO EM CSV')
df_solo.coalesce(1).write.save(path='C:/projeto/TCC-PUPUNHA/data/solo', format='csv', mode='overwrite', sep=';', header=True)
