library(ggplot2)
library(dplyr)
library(ggridges)

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

# Efetua limpeza dos registros pelas cidades presentes no vale do ribeira
suldoeste_met_clean <- suldoeste_met %>% filter(city == 'Pariquera-AÃ§u' 
                                                | city == 'Barra do Turvo'
                                                | city == 'Itariri'
                                                | city == 'CananÃ©ia'
                                                | city == 'Pedro de Toledo'
                                                | city == 'Iporanga'
                                                | city == 'Eldorado'
                                                | city == 'Miracatu'
                                                | city == 'Cajati'
                                                | city == 'Sete Barras'
                                                | city == 'JuquiÃ¡'
                                                | city == 'Jacupiranga'
                                                | city == 'Ilha Comprida'
                                                | city == 'Registro'
                                                | city == 'Iguape')

summary(suldoeste_met_clean)

# Quantidade de precipitacao por ano
suldoeste_met_clean %>%
  group_by(yr) %>% 
  summarise(precipitacao = sum(prcp, na.rm = TRUE)) %>% 
  ggplot() +
  geom_bar(aes(x = yr, y = precipitacao), stat = "identity", color = "black", fill = "light blue")

# máximo de radiacao por ano
suldoeste_met_clean %>%
  group_by(yr) %>% 
  summarise(radiacao = max(gbrd, na.rm = TRUE)) %>% 
  ggplot() +
  geom_bar(aes(x = yr, y = radiacao), stat = "identity", color = "black", fill = "yellow")

# Media de umidade por ano
suldoeste_met_clean %>%
  group_by(yr) %>% 
  summarise(umidade = mean(hmdy, na.rm = TRUE)) %>% 
  ggplot() +
  geom_bar(aes(x = yr, y = umidade), stat = "identity", color = "black", fill = "blue")
