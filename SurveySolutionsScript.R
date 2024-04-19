# importing the library
library("devtools")
devtools::install_github("arthur-shaw/susoapi")
library("susoapi")
library("tidyverse")
library("haven")
library("plyr")

library(DBI)
library(RMySQL)
library(janitor)

# creating a database connection
connection <- dbConnect(RMySQL::MySQL(), 
                        dbname = "survey_dashboards", 
                        host = "200.32.244.19", 
                        port = 3306, 
                        user = "gaguilar", 
                        password = "P@ssw0rd10")


set_credentials(
  server = "http://hqsurveys.sib.org.bz",
  workspace = "hbs",
  user = "gapi",
  password = "API4statistics!"
)

# get a dataframe of questionnaires and their attributes
# all_questionnaires <- get_questionnaires()

# STEP1: START AN EXPORT JOB
# specifying same same options as in user interface
# optionally specifying other options--including some not available in the UI
start_export(
  qnr_id = "8f467cac973940b184efec0944aea449$1",
  export_type = "SPSS",
  interview_status = "All",
  include_meta = TRUE
) -> started_job_id

# STEP 2: CHECK EXPORT JOB PROGESS, UNTIL COMPLETE
# specifying ID of job started in prior step

while (TRUE){
  res <- get_export_job_details(job_id = started_job_id)
  
  status <- res$ExportStatus 
  
  if (status == 'Completed'){
    break
  }
}


# STEP 3: DOWNLOAD THE EXPORT FILE, ONCE THE JOB IS COMPLETE
# specifying:
# - job ID
# - where to download the file
get_export_file(
  job_id = started_job_id,
 # path = "C:\\Users\\sib_temp\\Documents\\R Script\\Script"
 path = './'
  # path = "/home/rshiny/airflow/scripts"
)

# extracting into given folder

unzip('belize_hbs_4_1_SPSS_All.zip',
      exdir = './SPSS_FOLDER/')

# Load extracted files into R

belize_hbs_4 <- read_sav('./SPSS_FOLDER/belize_hbs_4.sav', user_na = TRUE)



names(belize_hbs_4)

belize_hbs_4 |>
  janitor::tabyl(ER9)

#
consumption_pattern_roster <- read_dta(dtaFile2)
FF1_Roster <- read_sav('./SPSS_FOLDER/FF1Roster.sav') 
FF2_Roster <- read_sav('./SPSS_FOLDER/FF2Roster.sav') 
FF3_Roster <- read_sav('./SPSS_FOLDER/FF3Roster.sav') 
FF4_Roster <- read_dta(dtaFile6)
FF5_Roster <- read_dta(dtaFile7)
FF6_Roster <- read_dta(dtaFile8)
FF7_Roster <- read_dta(dtaFile9)
FF8_Roster <- read_dta(dtaFile10)
FF9_Roster <- read_dta(dtaFile11)
FF10_Roster <- read_dta(dtaFile12)
FF11_Roster <- read_dta(dtaFile13)
FF12_Roster <- read_dta(dtaFile14)
FF13_Roster <- read_dta(dtaFile15)
FF14_Roster <- read_dta(dtaFile16)
FF15_Roster <- read_dta(dtaFile17)
FF16_Roster <- read_dta(dtaFile18)
FF17_Roster <- read_dta(dtaFile19)
FF18_Roster <- read_dta(dtaFile20)
FF19_Roster <- read_dta(dtaFile21)
FF20_Roster <- read_dta(dtaFile22)
listing_roster <- read_dta(dtaFile23)
TR2_1 <- read_dta(dtaFile24)
TR2_2 <- read_dta(dtaFile25)

listing_roster <- 
  listing_roster |>
  left_join(
    belize_hbs_4 |> select(interview__key, cluster, ED_number, CTV, urban_rural, district),
    by = 'interview__key'
  )


# Test if connection was made
# query <- "show tables";
# result <- dbGetQuery(connection,query);
# print(result)

# Create tables
# dbWriteTable(connection,"hbs_household",belize_hbs_4,overwrite=T)
# dbReadTable(connection, "hbs_household")

dbWriteTable(connection,"hbs_listing_roster",listing_roster,overwrite=T)
dbReadTable(connection, "hbs_listing_roster")

dbWriteTable(connection,"hbs_consumption_pattern_roster",consumption_pattern_roster,overwrite=T)
dbReadTable(connection, "hbs_consumption_pattern_roster")

dbWriteTable(connection,"hbs_transportation_1",TR2_1,overwrite=T)
dbReadTable(connection, "hbs_transportation_1")

dbWriteTable(connection,"hbs_transportation_2",TR2_2,overwrite=T)
dbReadTable(connection, "hbs_transportation_2")

# Joins FF_Roster together

# renaming columns and changing their type to factor
# renaming so that we can rbind them
# changing to factor so we can put them all into a single column

f1 <- FF1_Roster |>
  dplyr::rename(
    id = FF1Roster__id,
    B = FF1B,
    C = FF1C,
    D = FF1D,
    E = FF1E,
    F = FF1F
  ) |>
  mutate(
    id = paste0('FF1_', id),
    B = as_factor(B),
    C = as_factor(C),
    D = as_factor(D),
    E = as_factor(E),
    F = as_factor(F)
  )

f2 <- FF2_Roster |>
  dplyr::rename(
    id = FF2Roster__id,
    B = FF2B,
    C = FF2C,
    D = FF2D,
    E = FF2E,
    F = FF2F
  ) |>
  mutate(
    id = paste0('FF2_', id),
    B = as_factor(B),
    C = as_factor(C),
    D = as_factor(D),
    E = as_factor(E),
    F = as_factor(F)
  )

f3 <- FF3_Roster |>
  dplyr::rename(
    id = FF3Roster__id,
    B = FF3B,
    C = FF3C,
    D = FF3D,
    E = FF3E,
    F = FF3F
  ) |>
  mutate(
    id = paste0('FF3_', id),
    B = as_factor(B),
    C = as_factor(C),
    D = as_factor(D),
    E = as_factor(E),
    F = as_factor(F)
  )

# combine them into one data.frame
f_temp <- rbind(f1, f2, f3) 

# convert to long format and add unique ID for each item/roster/question

f_temp <- f_temp |>
  pivot_longer(
    cols = 4:8,
    names_to = 'Question',
    values_to = 'Answer'
  ) |>
  mutate(
    id_full = paste0(id, Question)
  ) |>
  relocate(
    id_full,
    .after = 'interview__id'
  ) |> select(-c(id, Question))
  
# widen into final format

f_final <- f_temp |>  
  pivot_wider(names_from = id_full, values_from = Answer)

FF1_Roster |>
  full_join(FF2_Roster, by = c('interview__key', 'interview__id')) |>
  full_join(FF3_Roster,  by = c('interview__key', 'interview__id'), relationship = 'many-to-many') |> View()

FF1 <- full_join(FF1_Roster, FF2_Roster, relationship = "many-to-many", by = c("interview__key", "interview__id"))
FF2 <- full_join(FF3_Roster, FF4_Roster, relationship = "many-to-many", by = c("interview__key", "interview__id"))
FF1and2 <- full_join(FF1, FF2, relationship = "many-to-many", by = c("interview__key", "interview__id"))

FF3 <- full_join(FF5_Roster, FF6_Roster, relationship = "many-to-many", by = c("interview__key", "interview__id"))
FF4 <- full_join(FF7_Roster, FF8_Roster, relationship = "many-to-many", by = c("interview__key", "interview__id"))
FF3and4 <- full_join(FF3, FF4, relationship = "many-to-many", by = c("interview__key", "interview__id"))

FF5 <- full_join(FF9_Roster, FF10_Roster, relationship = "many-to-many", by = c("interview__key", "interview__id"))
FF6 <- full_join(FF11_Roster, FF12_Roster, relationship = "many-to-many", by = c("interview__key", "interview__id"))
FF5and6 <- full_join(FF5, FF6, relationship = "many-to-many", by = c("interview__key", "interview__id"))

FF7 <- full_join(FF13_Roster, FF14_Roster, relationship = "many-to-many", by = c("interview__key", "interview__id"))
FF8 <- full_join(FF15_Roster, FF16_Roster, relationship = "many-to-many", by = c("interview__key", "interview__id"))
FF7and8 <- full_join(FF7, FF8, relationship = "many-to-many", by = c("interview__key", "interview__id"))

FF9 <- full_join(FF17_Roster, FF18_Roster, relationship = "many-to-many", by = c("interview__key", "interview__id"))
FF10 <- full_join(FF19_Roster, FF20_Roster, relationship = "many-to-many", by = c("interview__key", "interview__id"))
FF9and10 <- full_join(FF9, FF10, relationship = "many-to-many", by = c("interview__key", "interview__id"))

FF1to4 <- full_join(FF1and2, FF3and4, relationship = "many-to-many", by = c("interview__key", "interview__id"))
FF5to8 <- full_join(FF5and6, FF7and8, relationship = "many-to-many", by = c("interview__key", "interview__id"))

FF1to8 <- full_join(FF1to4, FF5to8, relationship = "many-to-many", by = c("interview__key", "interview__id"))

FF1to10 <- full_join(FF1to8, FF9and10, relationship = "many-to-many", by = c("interview__key", "interview__id"))

dbWriteTable(connection,"hbs_ff_roster",FF1to10,overwrite=T)
dbReadTable(connection, "hbs_ff_roster")

# Disconnect from database
dbDisconnect(connection)