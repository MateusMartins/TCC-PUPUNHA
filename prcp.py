import sys
sys.dont_write_bytecode=True
from pyspark.sql.functions import expr, col, max as _max, mean as _mean, sum as _sum, count as _count, min as _min, abs as _abs, lit
from pyspark.sql import SparkSession, Row, DataFrameWriter
from pyspark.sql.types import IntegerType, DoubleType

spark = SparkSession.builder.appName('prcp').config("spark.debug.maxToStringFields", 4000).master("local").enableHiveSupport().getOrCreate()

def get_metrics(df_in):
    # Efetua o agrupamento dos valores, efetuando a agregacao de determinados campos
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

def get_prcp_day(df_in):
    # Efetua o agrupamento dos valores, efetuando a agregacao de determinados campos
    print('CONSTRUINDO DATAFRAME COM VALORES AGREGADOS POR DIA')
    df_out = df_in.groupby('city','yr','mo', 'da').agg(
        _abs(_max('lat')).alias('latitude'),
        _sum('prcp').alias('prcp_dia'),
        _max('tmax').alias('tmax'),
        _min('tmin').alias('tmin'),
        _mean('temp').alias('med_temp')
        ).orderBy('city', 'yr', 'mo', 'da')
    return df_out

def get_et0(df_in):
    # Efetua o agrupamento dos valores, efetuando a agregacao de determinados campos
    print('CONSTRUINDO DATAFRAME COM VALORES AGREGADOS')
    df_out = df_in.groupby('city','yr','mo').agg(
        _mean('ET0').alias('ET0_MES')
        ).orderBy('city', 'yr', 'mo')
    return df_out

# Efetua a leitura do csv de clima
print('EFETUANDO LEITURA DO CSV DE CLIMA')
df_clima = spark.read.csv('C:/projeto/datasets/datasetSaoPaulo.csv', header=True, sep=',')

# Efetua a leitura do csv de parametros de ra
print('EFETUANDO LEITURA DO CSV DE PARAMETROS DE Ra')
df_parametro_ra = spark.read.csv('C:/projeto/datasets/parametroRa.csv', header=True, sep=';')

fields_list = ['prcp','temp','tmax','tmin']
print('ALTERANDO AS VARIAVEIS {} DO DATAFRAME DF_CLIMA PARA DOUBLE'.format(fields_list))
for name in fields_list:
    df_clima = df_clima.withColumn(name, df_clima[name].cast(DoubleType()))

print('ALTERANDO AS VARIAVEIS {} DO DATAFRAME DF_PARAMETRO_RA PARA DOUBLE'.format(df_parametro_ra.columns))
for name in df_parametro_ra.columns:
    df_parametro_ra = df_parametro_ra.withColumn(name, df_parametro_ra[name].cast(DoubleType()))

cidades_list = ['Pariquera-Açu', 'Barra do Turvo', 'Itariri', 'Cananéia', 'Pedro de Toledo', 'Iporanga', 'Eldorado', 'Miracatu', 'Cajati', 'Sete Barras', 'Juquiá', 'Jacupiranga', 'Ilha Comprida', 'Registro', 'Iguape']
# seleciona as cidades presentes na lista 'cidades'
print('SELECIONANDO CIDADES PRESENTES NO VALE DO RIBEIRA')
print(cidades_list)
df_clima = df_clima.filter(col('city').isin(cidades_list))

print('EFETUANDO LIMPEZA DOS VALORES DO DF_CLIMA')

# Remove os valores NULL ou '' dos campos de valor
clima_list = ['prcp','stp','smax','smin','gbrd','temp','dewp','tmax','dmax','tmin','dmin','hmdy','hmax','hmin','wdsp','wdct','gust']
print('SUBSTITUINDO VALORES NULOS OU VAZIOS POR 0 NOS CAMPOS A SEGUIR: {}'.format(clima_list))
for name in clima_list:
    df_clima = df_clima.withColumn(name, expr('if({} is null or trim({}) = "", 0, {})'.format(name, name, name)))

print('REMOVENDO CARACTERES ESPECIAIS DAS CIDADES')
df_clima = df_clima.withColumn('city', expr("""CASE
                                                    WHEN city == 'Pariquera-Açu' THEN 'Pariquera-Acu'
                                                    WHEN city == 'Cananéia' THEN 'Cananeia'
                                                    WHEN city == 'Juquiá' THEN 'Juquia'
                                                    ELSE city
                                                END"""))

# Coleta as informacoes de clima agregados por dia
df_prcp = get_prcp_day(df_clima)

# Altera o formato da coluna latitude para Inteiro
print('ALTERANDO O TIPO DA COLUNA "LATITUDE" PARA INTEIRO')
df_prcp = df_prcp.withColumn('latitude', df_prcp['latitude'].cast(IntegerType()))

print('EFETUANDO JOIN DOS DATAFRAMES DE PRECIPITACAO E TABELA DE RA')
df_evapo = df_prcp.join(df_parametro_ra, df_prcp['latitude'] == df_parametro_ra['latitude'], 'left')

print('CRIA A COLUNA RA A PARTIR DO VALOR DE CADA MES')
df_evapo = df_evapo.withColumn('Ra', expr("""CASE
                                                WHEN mo == 01 THEN Janeiro
                                                WHEN mo == 02 THEN Fevereiro
                                                WHEN mo == 03 THEN Marco
                                                WHEN mo == 04 THEN Abril
                                                WHEN mo == 05 THEN Maio
                                                WHEN mo == 06 THEN Junho
                                                WHEN mo == 07 THEN Julho
                                                WHEN mo == 08 THEN Agosto
                                                WHEN mo == 09 THEN Setembro
                                                WHEN mo == 10 THEN Outubro
                                                WHEN mo == 11 THEN Novembro
                                                WHEN mo == 12 THEN Dezembro
                                                ELSE 0
                                            END"""))

print('EFETUANDO O CALCULO DE ET0')
df_evapo = df_evapo.withColumn('ET0', expr('0.002565*(med_temp + 17.78)*(sqrt((Tmax-Tmin)))*Ra')) \
.withColumn('ET0', expr('if(ET0 is null or trim(ET0) = "", 0, ET0)'))

print('CRIANDO AS COLUNAS POR PERIODO COM VALOR DA EVAPOTRANSPIRACAO')
df_evapo_et0 = get_et0(df_evapo) \
.withColumn('1_6', expr('ET0_MES * 0.81')) \
.withColumn('7_13', expr('ET0_MES * 1.22')) \
.withColumn('14_16', expr('ET0_MES * 1.23')) \
.withColumn('17_20', expr('ET0_MES * 0.89')) \
.withColumn('21_24', expr('ET0_MES * 0.94')) \
.withColumn('25_27', expr('ET0_MES * 0.54'))

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

# Efetuando gravacao do dataframe em csv
print('SALVANDO DATAFRAME MANHA EM CSV')
df_manha.coalesce(1).write.save(path='C:/projeto/TCC-PUPUNHA/data/manha', format='csv', mode='overwrite', sep=';', header=True)

# Efetuando gravacao do dataframe em csv
print('SALVANDO DATAFRAME TARDE EM CSV')
df_tarde.coalesce(1).write.save(path='C:/projeto/TCC-PUPUNHA/data/tarde', format='csv', mode='overwrite', sep=';', header=True)

# Efetuando gravacao do dataframe em csv
print('SALVANDO DATAFRAME COMPLETO EM CSV')
df_completo.coalesce(1).write.save(path='C:/projeto/TCC-PUPUNHA/data/completo', format='csv', mode='overwrite', sep=';', header=True)

# Efetuando gravacao do dataframe em csv
print('SALVANDO DATAFRAME DE EVAPORACAO CSV')
df_evapo_et0.coalesce(1).write.save(path='C:/projeto/TCC-PUPUNHA/data/evapo_dia', format='csv', mode='overwrite', sep=';', header=True)
