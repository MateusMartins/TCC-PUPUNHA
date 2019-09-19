library(ggplot2)
library(dplyr)
library(ggridges)

# Efetua a leitura dos dados coletados do KAGGLE sobre temperatura na região Sudeste
met_manha <- read.csv("C:/projeto/TCC-PUPUNHA/data/manha/manha.csv", header=TRUE, sep=";")
summary(met_manha)

# Efetua a leitura dos dados coletados do KAGGLE sobre temperatura na região Sudeste
met_tarde <- read.csv("C:/projeto/TCC-PUPUNHA/data/tarde/tarde.csv", header=TRUE, sep=";")
summary(met_tarde)

# Efetua a leitura dos dados coletados do KAGGLE sobre temperatura na região Sudeste
met_full <- read.csv("C:/projeto/TCC-PUPUNHA/data/completo/completo.csv", header=TRUE, sep=";")
summary(met_full)

# Efetua a leitura dos dados de solo
solo <- read.csv("C:/projeto/datasets/Dataset-Solo.csv", header=TRUE, sep=";")
summary(solo)

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