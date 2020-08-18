library(tidyverse)
#load functions
fix_pun <- function(x) {
  x_names <- make.names(names(x))
  x_names <- gsub(x = x_names, pattern = '\\.\\.', replacement = '_')
  x_names <- gsub(x = x_names, pattern = '\\.', replacement = '_')
  x_names <- gsub(x = x_names, pattern = '\\_$', replacement = '')
  x_names
} #fix punctuation issues


#download (some) data
gama_link <- 'https://geotracker.waterboards.ca.gov/gama/data_download/gama_gama_statewide.zip'
usgs_link <- 'https://geotracker.waterboards.ca.gov/gama/data_download/gama_usgs_statewide.zip'
usgsnwis_link <- 'https://gamagroundwater.waterboards.ca.gov/gama/data_download/gama_usgsnwis_statewide.zip' #gama_usgsnew_statewide
dwr_link <- 'https://gamagroundwater.waterboards.ca.gov/gama/data_download/gama_dwr_statewide.zip'
#dpr_link <- 'https://gamagroundwater.waterboards.ca.gov/gama/data_download/gama_dpr_statewide.zip'
#llnl_link <- 'https://gamagroundwater.waterboards.ca.gov/gama/data_download/gama_llnl_statewide.zip'
ca_data <- data.frame()
link_list <- c(gama_link, usgs_link, usgsnwis_link, dwr_link)#, dpr_link, llnl_link)
filenames <- c("gama_gama_statewide.txt","gama_usgs_statewide.txt", "gama_usgsnew_statewide.txt", 'gama_dwr_statewide.txt')#, "gama_dpr_statewide.txt", "gama_llnl_statewide.txt")
for (n in 1:length(link_list)) {
  temp <- tempfile()
  download.file(link_list[n], temp)
  mytable <- readr::read_tsv(unz(temp, filenames[n]), guess_max = 1000)
  unlink(temp)
  names(mytable) <- fix_pun(mytable)
  mytable$WELL_ID <- as.character(mytable$WELL_ID)                      #force characters
  mytable$SOURCE_NAME <- as.character(mytable$SOURCE_NAME)
  mytable$OTHER_NAMES <- as.character(mytable$OTHER_NAMES)
  #mytable <- NA_clean(mytable)
  ca_data <- rbind(ca_data, mytable)                                    #grow big data table (continue appending new data)
  rm(mytable)
}

#print list of unique chemicals within dataset
unique(ca_data$CHEMICAL)

ca_data2 <- ca_data %>% filter(CHEMICAL %in% c("BIS2EHP","CR6","STY", "DIOXANE14", "NO3N", "AS", "U", "CR6", "TCPR123", "PCATE", "FE", "MN", "TDS", "PCE", "TCE", "DBCP"))
rm(ca_data)

ddw_link <- 'https://geotracker.waterboards.ca.gov/gama/data_download/gama_ddw_statewide.zip'
temp <- tempfile()
download.file(ddw_link, temp)
DDW <- readr::read_tsv(unz(temp, 'gama_ddw_statewide.txt'), guess_max = 1000)
unlink(temp)
names(DDW) <- fix_pun(DDW)                                              #fix punctuation in column headers
DDW2 <- DDW %>% filter(CHEMICAL %in% c("BIS2EHP","CR6","STY", "DIOXANE14","NO3N", "AS", "U", "CR6", "TCPR123", "PCATE", "FE", "MN", "TDS", "PCE", "TCE", "DBCP"))
DDW2 <- DDW2[!grepl("SPRING", DDW2$OTHER_NAMES),]                          #remove springs from DHS dataset

gw <- rbind(ca_data2, DDW2)
rm(DDW)

loc_link <- 'https://gamagroundwater.waterboards.ca.gov/gama/data_download/gama_location_construction_gis.zip'
temp <- tempfile()
download.file(loc_link, temp)
loc <- readr::read_tsv(unz(temp, 'gama_location_construction_gis.txt'), guess_max = 100000)
unlink(temp)
names(loc) <- fix_pun(loc)
loc_sm <- loc %>% select(WELL_ID, COUNTY, GROUNDWATER_BASIN, REGIONAL_BOARD, SENATE_DISTRICT,
                         HYDROGEOLOGIC_VULNERABLE_AREA, GAMA_STUDY_AREA, ASSEMBLY_DISTRICT, GROUNDWATER_SUSTAINABILITY_AGENCY, DEPARTMENT_OF_WATER_RESOURCES_REGION)

#join and make sure data doesn't have missing values
gw <- left_join(gw, loc_sm)
gw_fil <- gw %>% filter(!is.na(RESULTS), !is.na(COUNTY), !is.na(DATE))
gw_fil$DATE <- lubridate::mdy(gw_fil$DATE)
unique(gw_fil$CHEMICAL)

#fix unrenderable tilde
gw_fil <- gw_fil %>% dplyr::mutate(GROUNDWATER_BASIN = ifelse(str_detect(GROUNDWATER_BASIN, "NUEVO AREA"), "ANO NUEVO AREA (3-020)", GROUNDWATER_BASIN))
#gw_fil$GROUNDWATER_BASIN <- stringi::stri_trans_general(str = gw_fil$GROUNDWATER_BASIN,
#id = "Latin-ASCII")

write.csv(gw_fil, "H:/R/GAMA_trends_by_countygw_full_3_2_2020.csv", row.names = F)

