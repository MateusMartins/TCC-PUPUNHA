library(ggplot2)
library(dplyr)
library(ggridges)
library(hrbrthemes)

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

# Maior media de temperatura anual registrada por cidade durante a manha
met_manha %>%
  group_by(city) %>% 
  summarise(med_temp = max(med_temp, na.rm = TRUE)) %>% 
  ggplot() +
  geom_bar(aes(x = city, y = med_temp), stat = "identity", color = "black", fill = "light blue")

# Maior media de temperatura anual registrada por cidade durante a tarde
met_tarde %>%
  group_by(city) %>% 
  summarise(med_temp = max(med_temp, na.rm = TRUE)) %>% 
  ggplot() +
  geom_bar(aes(x = city, y = med_temp), stat = "identity", color = "black", fill = "light yellow")

# Maior media de temperatura anual registrada por cidade durante o dia inteiro
met_full %>%
  group_by(city) %>% 
  summarise(med_temp = max(med_temp, na.rm = TRUE)) %>% 
  ggplot() +
  geom_bar(aes(x = city, y = med_temp), stat = "identity", color = "black", fill = "light green")

# Menor media de temperatura anual registrada por cidade durante a manha
met_manha %>%
  group_by(city) %>% 
  summarise(med_temp = min(med_temp, na.rm = TRUE)) %>% 
  ggplot() +
  geom_bar(aes(x = city, y = med_temp), stat = "identity", color = "black", fill = "light blue")

# Menor media de temperatura anual registrada por cidade durante a tarde
met_tarde %>%
  group_by(city) %>% 
  summarise(med_temp = min(med_temp, na.rm = TRUE)) %>% 
  ggplot() +
  geom_bar(aes(x = city, y = med_temp), stat = "identity", color = "black", fill = "light yellow")

# Menor media de temperatura anual registrada por cidade durante o dia inteiro
met_full %>%
  group_by(city) %>% 
  summarise(med_temp = min(med_temp, na.rm = TRUE)) %>% 
  ggplot() +
  geom_bar(aes(x = city, y = med_temp), stat = "identity", color = "black", fill = "light green")

# Altura maxima das cidades em analise
met_full %>%
  group_by(city) %>% 
  summarise(alt_max = max(ele_max, na.rm = TRUE)) %>% 
  ggplot() +
  geom_bar(aes(x = city, y = alt_max), stat = "identity", color = "black", fill = "light gray")

# Media de temperatura ao longo do tempo na cidade de registro
met_full %>%
  filter(city == 'Registro') %>%
    ggplot( aes(x=yr, y=med_temp)) +
      geom_area( fill="#69b3a2", alpha=0.4) +
      geom_line(color="#69b3a2", size=2) +
      geom_point(size=3, color="#69b3a2") +
      theme_ipsum() +
      ggtitle("Precipitação ao longo do tempo na cidade de Registro")

# precipitacao ao longo do tempo na cidade de registro
met_full %>%
  filter(city == 'Registro') %>%
    ggplot( aes(x=yr, y=prcp)) +
      geom_area( fill="#69b3a2", alpha=0.4) +
      geom_line(color="#69b3a2", size=2) +
      geom_point(size=3, color="#69b3a2") +
      theme_ipsum() +
      ggtitle("Precipitação ao longo do tempo na cidade de Registro")


