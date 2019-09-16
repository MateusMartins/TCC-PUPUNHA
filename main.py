import sys
sys.dont_write_bytecode=True
from pyspark.sql.functions import expr, col, max as _max, mean as _mean, sum as _sum
from pyspark.sql import SparkSession, Row, DataFrameWriter

spark = SparkSession.builder.appName('abc').config("spark.debug.maxToStringFields", 4000).master("local").enableHiveSupport().getOrCreate()

# Efetua a leitura do csv de clima
print('EFETUANDO LEITURA DO CSV DE CLIMA')
df_clima = spark.read.csv('C:/projeto/datasets/datasetSaoPaulo.csv', header=True, sep=',')

cidades = ['Pariquera-Açu', 'Barra do Turvo', 'Itariri', 'Cananéia', 'Pedro de Toledo', 'Iporanga', 'Eldorado', 'Miracatu', 'Cajati', 'Sete Barras', 'Juquiá', 'Jacupiranga', 'Ilha Comprida', 'Registro', 'Iguape']

# seleciona as cidades presentes na lista 'cidades'
print('SELECIONANDO CIDADES PRESENTES NO VALE DO RIBEIRA')
df_clima = df_clima.filter(col('city').isin(cidades))

# Remove os valores NULL ou '' dos campos de valor
_ = ['prcp','stp','smax','smin','gbrd','temp','dewp','tmax','dmax','tmin','dmin','hmdy','hmax','hmin','wdsp','wdct','gust']
print('EFETUANDO LIMPEZA DOS VALORES')
for name in _:
    df_clima = df_clima.withColumn(name, expr('if({} is null or trim({}) = "", 0, {})'.format(name, name, name)))

# Efetua os calculos necessarios para realizar as analises
print('CONSTRUINDO DATAFRAME COM VALORES AGREGADOS')
df_clima_agg = df_clima.groupby('yr','city').agg(
    _max('elvt').alias('Elevação Máxima'),
    _mean('temp').alias('Média de temperatura'),
    _mean('tmin').alias('Média de temperatura Mínima'),
    _mean('tmax').alias('Média de temperatura Máxima'),
    _sum('prcp').alias('Precipitação por ano'),
    _mean('hmdy').alias('Média de umidade'),
    _mean('hmin').alias('Média de umidade Mínima'),
    _mean('hmax').alias('Média de umidade Máxima'),
    _mean('wdsp').alias('Média da velocidade do Vento'),
    _mean('gust').alias('Média da velocidade das rajadas de Vento')).orderBy('yr', 'city').persist()
df_clima_agg.count()

# Efetuando gravação do dataframe em csv
print('SALVANDO DATAFRAME EM CSV')
df_clima_agg.write.save(path='C:/projeto/TCC-PUPUNHA/data/file2.csv', format='csv', mode='overwrite', sep=';', header=True)
