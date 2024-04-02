# importing the library
library("devtools")
devtools::install_github("arthur-shaw/susoapi")
library("susoapi")
library("tidyverse")
library("haven")
library("plyr")

library(DBI)
library(RMySQL)

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
  export_type = "STATA",
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
  path = "C:\\Users\\sib_temp\\Documents\\R Script\\Script"
  # path = "/home/rshiny/airflow/scripts"
)

# Extract dta files out of zip
dtaFile1 = unz("belize_hbs_4_1_STATA_All.zip", "belize_hbs_4.dta")
dtaFile2 = unz("belize_hbs_4_1_STATA_All.zip", "consumption_pattern_roster.dta")
dtaFile3 = unz("belize_hbs_4_1_STATA_All.zip", "FF1Roster.dta")
dtaFile4 = unz("belize_hbs_4_1_STATA_All.zip", "FF2Roster.dta")
dtaFile5 = unz("belize_hbs_4_1_STATA_All.zip", "FF3Roster.dta")
dtaFile6 = unz("belize_hbs_4_1_STATA_All.zip", "FF4Roster.dta")
dtaFile7 = unz("belize_hbs_4_1_STATA_All.zip", "FF5Roster.dta")
dtaFile8 = unz("belize_hbs_4_1_STATA_All.zip", "FF6Roster.dta")
dtaFile9 = unz("belize_hbs_4_1_STATA_All.zip", "FF7Roster.dta")
dtaFile10 = unz("belize_hbs_4_1_STATA_All.zip", "FF8Roster.dta")
dtaFile11 = unz("belize_hbs_4_1_STATA_All.zip", "FF9Roster.dta")
dtaFile12 = unz("belize_hbs_4_1_STATA_All.zip", "FF10Roster.dta")
dtaFile13 = unz("belize_hbs_4_1_STATA_All.zip", "FF11Roster.dta")
dtaFile14 = unz("belize_hbs_4_1_STATA_All.zip", "FF12Roster.dta")
dtaFile15 = unz("belize_hbs_4_1_STATA_All.zip", "FF13Roster.dta")
dtaFile16 = unz("belize_hbs_4_1_STATA_All.zip", "FF14Roster.dta")
dtaFile17 = unz("belize_hbs_4_1_STATA_All.zip", "FF15Roster.dta")
dtaFile18 = unz("belize_hbs_4_1_STATA_All.zip", "FF16Roster.dta")
dtaFile19 = unz("belize_hbs_4_1_STATA_All.zip", "FF17Roster.dta")
dtaFile20 = unz("belize_hbs_4_1_STATA_All.zip", "FF18Roster.dta")
dtaFile21 = unz("belize_hbs_4_1_STATA_All.zip", "FF19Roster.dta")
dtaFile22 = unz("belize_hbs_4_1_STATA_All.zip", "FF20Roster.dta")
dtaFile23 = unz("belize_hbs_4_1_STATA_All.zip", "listingroster.dta")
dtaFile24 = unz("belize_hbs_4_1_STATA_All.zip", "TR2_1.dta")
dtaFile25 = unz("belize_hbs_4_1_STATA_All.zip", "TR2_2.dta")

# Load new data frames from data in dta file
belize_hbs_4 <- read_dta(dtaFile1)
consumption_pattern_roster <- read_dta(dtaFile2)
FF1_Roster <- read_dta(dtaFile3)
FF2_Roster <- read_dta(dtaFile4)
FF3_Roster <- read_dta(dtaFile5)
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