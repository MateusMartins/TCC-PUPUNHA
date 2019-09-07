library(ggplot2)
library(dplyr)

# Efetua a leitura dos dados coletados do site INMET com as informações meteorologiocas da cidade de iguape
iguape_met <- read_csv(file="C:/projeto/datasets/Iguape-2018-2019.csv", header=TRUE, sep=";")
summary(iguape_met)

# Efetua a leitura dos dados coletados do KAGGLE sobre temperatura na região Sudeste
suldoeste_met <- read.csv("C:/projeto/datasets/datasetSaoPaulo.csv", header=TRUE, sep=",")
summary(suldoeste_met)

# Efetua a leitura dos dados de solo
solo <- read.csv("C:/projeto/datasets/Dataset-Solo.csv", header=TRUE, sep=";")
summary(solo)

# Efetua a limpeza dos campos que contem '////'
iguape_met_clean <- iguape_met %>% filter(temp_inst != '////' | precipitacao != '////')

# Efetua limpeza dos registros acima de 2015
suldoeste_met_clean <- suldoeste_met %>% filter(yr > 2015)

plot(iguape_met_clean$precipitacao)

aggregate(temp_inst ~ codigo_estacao, iguape_met_clean, sum)
