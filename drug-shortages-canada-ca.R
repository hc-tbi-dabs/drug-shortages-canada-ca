#' Question: Identify the percentage of actual drug shortages reported to Health
#' Canada that did not have a prior report of an anticipated or actual shortage
#' within a 12 month period for the same drug.
#'
#' For those drugs that reported only an actual shortage, what was the average
#' age on the market*?
#'
#' *Age on the market is defined as having a current status of "Marketed", and
#' difference of today's date and "Original Market Date".
#'
#' To solve this you basically have to grab the extracts from both sites, the
#' key is going to be the DIN, which is referenced in both extracts that can be
#' used as the common key across the sets. 
#' 
#' First identify all those that had a shortage, then which ones didn't have a
#' prior report.
#'
#' Get that list and cross with DPD data using the DIN and do the date
#' calculations.
#' 

library(tidyverse)
library(httr)
library(jsonlite)
library(lubridate)
library(magrittr)
library(stringr)


#' Data from: https://www.drugshortagescanada.ca/

drug_shortages_data_1 <- read_csv("./data/drugshortagescanda-01-01-2016-to-01-01-2018.csv")
drug_shortages_data_2 <- read_csv("./data/drugshortagescanda-01-01-2018-to-01-01-2021.csv")
drug_shortages_data <- rbind(drug_shortages_data_1, drug_shortages_data_2)


#'Records 

actual_shortage <- drug_shortages_data %>%
  filter(`Shortage status` == "Actual shortage")

actual_shortage_not_anticipated <- drug_shortages_data %>%
  filter(`Shortage status` == "Actual shortage") %>%
  filter(is.na(`Anticipated start date`))

anticipated_shortage <- drug_shortages_data %>%
  filter(`Shortage status` == "Anticipated shortage")

actual_shortage_not_anticipated_DIN <- actual_shortage_not_anticipated$`Drug Identification Number`

#' Data from: https://www.canada.ca/en/health-canada/services/drugs-health-products/drug-products/drug-product-database/what-data-extract-drug-product-database.html
#'
#' Data does not include metadata, that is found here:
#' 
#' NOTE:  HISTORY_DATE is Original market date
#' 
#' https://www.canada.ca/en/health-canada/services/drugs-health-products/drug-products/drug-product-database/read-file-drug-product-database-data-extract.html

colnames_comp <- c(
  "DRUG_CODE",
  "MFR_CODE",
  "COMPANY_CODE",
  "COMPANY_NAME",
  "COMPANY_TYPE",
  "ADDRESS_MAILING_FLAG",
  "ADDRESS_BILLING_FLAG",
  "ADDRESS_NOTIFICATION_FLAG",
  "ADDRESS_OTHER",
  "SUITE_NUMBER",
  "STREET_NAME",
  "CITY_NAME",
  "PROVINCE",
  "COUNTRY",
  "POSTAL_CODE",
  "POST_OFFICE_BOX",
  "PROVINCE_F",
  "COUNTRY_F"
)

colnames_drug <- c(
  "DRUG_CODE",
  "PRODUCT_CATEGORIZATION",
  "CLASS",
  "DRUG_IDENTIFICATION_NUMBER",
  "BRAND_NAME",
  "DESCRIPTOR",
  "PEDIATRIC_FLAG",
  "ACCESSION_NUMBER",
  "NUMBER_OF_AIS",
  "LAST_UPDATE_DATE",
  "AI_GROUP_NO",
  "CLASS_F",
  "BRAND_NAME_F",
  "DESCRIPTOR_F"
)

colnames_form <- c(
  "DRUG_CODE",
  "PHARM_FORM_CODE",
  "PHARMACEUTICAL_FORM",
  "PHARMACEUTICAL_FORM_F"
)

colnames_ingred <- c(
  "DRUG_CODE",
  "ACTIVE_INGREDIENT_CODE",
  "INGREDIENT",
  "INGREDIENT_SUPPLIED_IND",
  "STRENGTH",
  "STRENGTH_UNIT",
  "STRENGTH_TYPE",
  "DOSAGE_VALUE",
  "BASE",
  "DOSAGE_UNIT",
  "NOTES",
  "INGREDIENT_F",
  "STRENGTH_UNIT_F",
  "STRENGTH_TYPE_F",
  "DOSAGE_UNIT_F"
)

colnames_package <- c(
  "DRUG_CODE",
  "UPC",
  "PACKAGE_SIZE_UNIT",
  "PACKAGE_TYPE",
  "PACKAGE_SIZE",
  "PRODUCT_INFORMATION",
  "PACKAGE_SIZE_UNIT_F",
  "PACKAGE_TYPE_F",
)

colnames_pharm <- c(
  "DRUG_CODE",
  "PHARMACEUTICAL_STD"
)

colnames_route <- c(
  "DRUG_CODE",
  "ROUTE_OF_ADMINISTRATION_CODE",
  "ROUTE_OF_ADMINISTRATION",
  "ROUTE_OF_ADMINISTRATION_F"
)
colnames_schedule <-c(
  "DRUG_CODE",
  "SCHEDULE",
  "SCHEDULE_F"
)

colnames_status <- c(
  "DRUG_CODE",
  "CURRENT_STATUS_FLAG",
  "STATUS",
  "HISTORY_DATE",
  "STATUS_F",
  "LOT_NUMBER",
  "EXPIRATION_DATE"
)

colnames_ther <-c(
  "DRUG_CODE",
  "TC_ATC_NUMBER",
  "TC_ATC",
  "TC_AHFS_NUMBER",
  "TC_AHFS",
  "TC_ATC_F",
  "TC_AHFS_F"
)

colnames_vet <- c(
  "DRUG_CODE",
  "VET_SPECIES",
  "VET_SUB_SPECIES",
  "VET_SPECIES_F"
)

#' Data locations:

data_comp <- "./data/DPD/allfiles/comp.txt"
data_drug <- "./data/DPD/allfiles/drug.txt"
data_form <- "./data/DPD/allfiles/form.txt"
data_ingred <- "./data/DPD/allfiles/ingred.txt"
data_package <- "./data/DPD/allfiles/package.txt"
data_pharm <- "./data/DPD/allfiles/pharm.txt"
data_route <- "./data/DPD/allfiles/route.txt"
data_schedule <- "./data/DPD/allfiles/schedule.txt"
data_status <- "./data/DPD/allfiles/status.txt"
data_ther <- "./data/DPD/allfiles/ther.txt"
data_vet <- "./data/DPD/allfiles/vet.txt"

load_data_with_cols <- function(data, cols) {
  df <- read_csv(data, col_names = F)
  colnames(df) <- cols
  return(df)
}


#' Load data:

drug_product_database_all_files_comp <- load_data_with_cols(data_comp, colnames_comp)
drug_product_database_all_files_drug <- load_data_with_cols(data_drug, colnames_drug)
drug_product_database_all_files_form <- load_data_with_cols(data_form, colnames_form)
drug_product_database_all_files_ingred <- load_data_with_cols(data_ingred, colnames_ingred)
drug_product_database_all_files_package <- load_data_with_cols(data_package, colnames_package)
drug_product_database_all_files_pharm <- load_data_with_cols(data_pharm, colnames_pharm)
drug_product_database_all_files_route <- load_data_with_cols(data_route, colnames_route)
drug_product_database_all_files_schedule <- load_data_with_cols(data_schedule, colnames_schedule)
drug_product_database_all_files_status <- load_data_with_cols(data_status, colnames_status)
drug_product_database_all_files_ther <- load_data_with_cols(data_ther, colnames_ther)
drug_product_database_all_files_vet <- load_data_with_cols(data_vet, colnames_vet)


#' Mapping between DIN and DRUG_CODE:

din_drug_code_mapping <- drug_product_database_all_files_drug %>% select(DRUG_CODE, DRUG_IDENTIFICATION_NUMBER)

#' Add DIN to table of interest:

my_drug_product_database_all_files_status <- merge(drug_product_database_all_files_status,
                                                   din_drug_code_mapping,
                                                   by.y = "DRUG_CODE")


#' Ensure types are the same:
my_drug_product_database_all_files_status$DRUG_IDENTIFICATION_NUMBER %<>%
  as.character(my_drug_product_database_all_files_status$DRUG_IDENTIFICATION_NUMBER)

actual_shortage_not_anticipated$`Drug Identification Number` %<>%
  as.character(actual_shortage_not_anticipated$`Drug Identification Number`) %>%
  str_pad(8, pad = "0")

my_drug_product_database_status <- merge(my_drug_product_database_all_files_status,
                                         actual_shortage_not_anticipated,
                                         by.x = "DRUG_IDENTIFICATION_NUMBER",
                                         by.y = "Drug Identification Number")


#' Find relevant data and calculate time on market:

my_drug_product_database_status_marketed <- my_drug_product_database_status %>%
  filter(STATUS == "MARKETED") %>%
  mutate(HISTORY_DATE = dmy(HISTORY_DATE)) %>%
  mutate(AGE_ON_MARKET = lubridate::today() - HISTORY_DATE)


#' Answers to Questions:

print("Percentage of of actual drug shortages reported to Health Canada that did not have a prior report of an anticipated or actual shortage within a 12 month period for the same drug.")
print(actual_shortage_not_anticipated %>% nrow() / actual_shortage %>% nrow())

print("For those drugs that reported only an actual shortage, what was the average age on the market?")
print(my_drug_product_database_status_marketed$AGE_ON_MARKET %>% mean())
