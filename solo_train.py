import sys
sys.dont_write_bytecode=True
from pyspark.sql.functions import expr, col, max as _max, mean as _mean, sum as _sum
from pyspark.sql import SparkSession, Row, DataFrameWriter

spark = SparkSession.builder.appName('Train Test - solo').config("spark.debug.maxToStringFields", 4000).master("local").enableHiveSupport().getOrCreate()

def set_range(df_in, column):
    if column == "k":
        df_out = df_in.withColumn('K_value', expr("""CASE
                                                        WHEN K >= 0 AND K < 61 THEN 0.25
                                                        WHEN K >= 61 AND K < 121 THEN 0.5
                                                        WHEN K >= 121 AND K < 181 THEN 1
                                                        WHEN K >= 181 AND K < 241 THEN 0.5
                                                        WHEN K >= 241 THEN 0.25
                                                        ELSE 0
                                                    END"""))
        return df_out
    elif column == "n":
        df_out = df_in.withColumn('N_value', expr("""CASE
                                                        WHEN N >= 0 AND N < 61 THEN 0.25
                                                        WHEN N >= 61 AND N < 121 THEN 0.5
                                                        WHEN N >= 121 AND N < 181 THEN 1
                                                        WHEN N >= 181 AND N < 241 THEN 0.5
                                                        WHEN N >= 241 THEN 0.25
                                                        ELSE 0
                                                    END"""))
        return df_out
    elif column == "ca":
        df_out = df_in.withColumn('Ca_value', expr("""CASE
                                                        WHEN Ca >= 0 AND Ca < 31 THEN 0.25
                                                        WHEN Ca >= 31 AND Ca < 61 THEN 0.5
                                                        WHEN Ca >= 61 AND Ca < 91 THEN 1
                                                        WHEN Ca >= 91 AND Ca < 121 THEN 0.5
                                                        WHEN Ca >= 121 THEN 0.25
                                                        ELSE 0
                                                    END"""))
        return df_out
    elif column == "p":
        df_out = df_in.withColumn('P_value', expr("""CASE
                                                        WHEN P >= 0 AND P < 16 THEN 0.25
                                                        WHEN P >= 16 AND P < 31 THEN 0.5
                                                        WHEN P >= 31 AND P < 46 THEN 1
                                                        WHEN P >= 46 AND P < 61 THEN 0.5
                                                        WHEN P >= 61 THEN 0.25
                                                        ELSE 0
                                                    END"""))
        return df_out
    elif column == "mg":
        df_out = df_in.withColumn('Mg_value', expr("""CASE
                                                        WHEN Mg >= 0 AND Mg < 12.6 THEN 0.25
                                                        WHEN Mg >= 12.6 AND Mg < 25 THEN 0.5
                                                        WHEN Mg >= 25 AND Mg < 41 THEN 1
                                                        WHEN Mg >= 41 AND Mg < 56 THEN 0.5
                                                        WHEN Mg >= 56 THEN 0.25
                                                        ELSE 0
                                                    END"""))
        return df_out
    else:
        print('COLUNA INFORMADA NAO EXISTE')
        return df_out

def set_value(df_in):
    df_out = df_in.withColumn('mean_value', expr('((K_value*5)+(N_value*4)+(Ca_value*3)+(P_value*2)+(Mg_value*1))/(5+4+3+2+1)'))
    return df_out

def set_class(df_in):
    df_out = df_in.withColumn('class', expr("""CASE
                                                        WHEN mean_value >= 0 AND mean_value <= 0.25 THEN "Pessimo"
                                                        WHEN mean_value > 0.25 AND mean_value <= 0.5 THEN "Ruim"
                                                        WHEN mean_value > 0.5 AND mean_value <= 0.75 THEN "Bom"
                                                        WHEN mean_value > 0.75 AND mean_value <= 1 THEN "Ideal"
                                                        ELSE "Erro"
                                                    END"""))
    return df_out

# Efetua a leitura do csv de clima
print('EFETUANDO LEITURA DO CSV DE SOLO')
df_train = spark.read.csv('C:/projeto/datasets/solo_train.csv', header=True, sep=';')

for name in df_train.columns:
    df_train = set_range(df_train, name.lower())

df_train = set_value(df_train)
df_train = set_class(df_train)

drop_list = ['mean_value', 'Mg_value', 'P_value', 'Ca_value', 'N_value', 'K_value']
for name in drop_list:
    df_train = df_train.drop(name)

# Efetuando gravação do dataframe em csv
print('SALVANDO DATAFRAME SOLO EM CSV')
df_train.coalesce(1).write.save(path='C:/projeto/TCC-PUPUNHA/data/solo_train', format='csv', mode='overwrite', sep=';', header=True)
