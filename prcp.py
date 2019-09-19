import sys
sys.dont_write_bytecode=True
from pyspark.sql.functions import expr, col, max as _max, mean as _mean, sum as _sum
from pyspark.sql import SparkSession, Row, DataFrameWriter

spark = SparkSession.builder.appName('prcp').config("spark.debug.maxToStringFields", 4000).master("local").enableHiveSupport().getOrCreate()

def get_metrics(df_in):
    # Efetua os calculos necessarios para realizar as analises
    print('CONSTRUINDO DATAFRAME COM VALORES AGREGADOS')
    df_out = df_in.groupby('yr','city').agg(
        _max('elvt').alias('ele_max'),
        _mean('temp').alias('med_temp'),
        _mean('tmin').alias('med_temp_min'),
        _mean('tmax').alias('med_temp_max'),
        _sum('prcp').alias('prcp'),
        _mean('hmdy').alias('med_umi'),
        _mean('hmin').alias('med_umi_min'),
        _mean('hmax').alias('med_umi_max'),
        _mean('wdsp').alias('med_velo_vento'),
        _mean('gust').alias('med_velo_rajadas_vento')).orderBy('yr', 'city')
    return df_out

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
df_clima = df_clima.withColumn('city', expr("""CASE
                                                    WHEN city == 'Pariquera-Açu' THEN 'Pariquera-Acu'
                                                    WHEN city == 'Cananéia' THEN 'Cananeia'
                                                    WHEN city == 'Juquiá' THEN 'Juquia'
                                                    ELSE city
                                                END"""))

print('CRIANDO DATAFRAME PARA O PERIODO DA MANHA')
df_manha = df_clima.filter('hr < 12')

print('CRIANDO DATAFRAME PARA O PERIODO DA TARDE')
df_tarde = df_clima.filter('hr >= 12')

print('CRIANDO DATAFRAME COM DADOS DA MANHA')
df_manha = get_metrics(df_manha)

print('CRIANDO DATAFRAME COM DADOS DA TARDE')
df_tarde = get_metrics(df_tarde)

print('CRIANDO DATAFRAME COM TODOS OS DADOS')
df_completo = get_metrics(df_clima)

# Efetuando gravação do dataframe em csv
print('SALVANDO DATAFRAME MANHA EM CSV')
df_manha.coalesce(1).write.save(path='C:/projeto/TCC-PUPUNHA/data/manha', format='csv', mode='overwrite', sep=';', header=True)

# Efetuando gravação do dataframe em csv
print('SALVANDO DATAFRAME TARDE EM CSV')
df_tarde.coalesce(1).write.save(path='C:/projeto/TCC-PUPUNHA/data/tarde', format='csv', mode='overwrite', sep=';', header=True)

# Efetuando gravação do dataframe em csv
print('SALVANDO DATAFRAME COMPLETO EM CSV')
df_completo.coalesce(1).write.save(path='C:/projeto/TCC-PUPUNHA/data/completo', format='csv', mode='overwrite', sep=';', header=True)
