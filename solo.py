import sys
sys.dont_write_bytecode=True
from pyspark.sql.functions import expr, col, max as _max, mean as _mean, sum as _sum
from pyspark.sql import SparkSession, Row, DataFrameWriter

spark = SparkSession.builder.appName('solo').config("spark.debug.maxToStringFields", 4000).master("local").enableHiveSupport().getOrCreate()

# Efetua a leitura do csv de clima
print('EFETUANDO LEITURA DO CSV DE CLIMA')
df_solo = spark.read.csv('C:/projeto/datasets/datasetSolo.csv', header=True, sep=';')

print('ALTERANDO COLUNAS PARA DOUBLE')
for column in df_solo.columns:
    df_solo = df_solo.withColumn(column, df_solo[column].cast("Double"))
df_solo.printSchema()

element_list = ['K', 'Ca', 'Mg']
print('SELECIONANDO ELEMENTOS: {}'.format(element_list))
df_treino = df_solo.select(element_list)

print('EFETUANDO TRANSFORMACAO DE Mmolc/dm³ -> cmol/dm³')
for column in df_treino.columns:
    df_treino = df_treino.withColumn(column, expr("{}/10".format(column)))

print('EFETUANDO TRANSFORMACAO DE cmol/dm³ -> mg/dm³')
print('ALTERANDO A AMOSTRA DE POTASSIO')
df_treino = df_treino.withColumn('K', expr("K*391"))

print('ALTERANDO A AMOSTRA DE CALCIO')
df_treino = df_treino.withColumn('Ca', expr("Ca*200.4"))

print('ALTERANDO A AMOSTRA DE MAGNESIO')
df_treino = df_treino.withColumn('Mg', expr("Mg*230"))

print('EFETUANDO TRANSFORMACAO DE mg/dm³ -> Kg/ha')
for column in df_treino.columns:
    df_treino = df_treino.withColumn(column, expr("{}*2".format(column)))
df_treino.show(20, False)

print('EFETUANDO CALCULOS DE CATIONIZACAO')
df_ctc = df_solo.withColumn('SB', (col('K') + col('Ca') + col('Mg'))) \
    .withColumn('CTC_efetiva', (col('SB') + col('Al'))) \
    .withColumn('CTC_total', (col('Ca') + col('Mg') + col('K') + col('H+Al'))) \
    .withColumn('V', (100*(col('SB')/col('CTC_total'))))
df_ctc.show(10, False)

# Efetuando gravação do dataframe em csv
print('SALVANDO DATAFRAME SOLO EM CSV')
df_ctc.coalesce(1).write.save(path='C:/projeto/TCC-PUPUNHA/data/solo', format='csv', mode='overwrite', sep=';', header=True)

# Efetuando gravação do dataframe em csv
print('SALVANDO DATAFRAME SOLO EM CSV')
df_treino.coalesce(1).write.save(path='C:/projeto/TCC-PUPUNHA/data/solo_treino', format='csv', mode='overwrite', sep=';', header=True)
