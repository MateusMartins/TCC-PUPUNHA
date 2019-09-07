library(ggplot2)
library(dplyr)

# Efetua a leitura dos dados coletados do site INMET com as informações meteorologiocas da cidade de iguape
iguape_met <- read.csv(file="C:/projeto/datasets/Iguape-2018-2019.csv", header=TRUE, sep=";")

# Efetua a leitura dos dados coletados do KAGGLE sobre temperatura na região Sudeste
suldoeste_met <- read.csv("C:/projeto/datasets/datasetSaoPaulo.csv", header=TRUE, sep=",")

# Efetua a leitura dos dados de solo
solo <- read.csv("C:/projeto/datasets/Dataset-Solo.csv", header=TRUE, sep=";")

# Efetua a limpeza dos campos que contem '////'
iguape_met_clean <- iguape_met %>% filter(temp_inst != "////")

# Efetua limpeza dos registros pelos ano de 2016
suldoeste_met_clean <- suldoeste_met %>% filter(yr > 2015)


strDates <- as.character(iguape_met_clean$data)

x <- strsplit(strDates, "/")

View(x)
teste <- iguape_met_clean %>% separate()

