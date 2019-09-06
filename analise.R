# Efetua a leitura dos dados coletados do site INMET com as informações meteorologiocas da cidade de iguape
iguape_met <- read.csv(file="/home/mateus/Desktop/datasets/Iguape-2018-2019.csv", header=TRUE, sep=";")

# Efetua a leitura dos dados coletados do KAGGLE sobre temperatura na região Sudeste
sp_met <- read.csv("/home/mateus/Desktop/datasets/datasetSaoPaulo.csv", header=TRUE, sep=",")

# Efetua a limpeza dos campos que contem '////'
iguape_met_clean <- iguape_met %>% filter(temp_inst != "////")

# Efetua limpeza dos registros pelos ano de 2016
sp_met_clean <- sp_met %>% filter(yr > 2015)

