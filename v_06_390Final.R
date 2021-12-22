library(data.table)
library(tidyverse)
library(lubridate)

#=============================================================================#
# Q1. Download Data                                                           #
#=============================================================================#

# Create Directory for Raw Data

setwd("C:\\Users\\ahar1\\Desktop")
  ## Change to your directory

if (!dir.exists("390FP")) {
  dir.create("390FP")
}

setwd("C:\\Users\\ahar1\\Desktop\\390FP")

if (!dir.exists("hcris_raw")) {
  dir.create("hcris_raw")
}

# Download Data

start_year = 1998
end_year   = 2010

rpt_url_head = "http://www.nber.org/hcris/265-94/rnl_rpt265_94_"
rpt_url_tail = ".csv"

nmrc_url_head = "http://www.nber.org/hcris/265-94/rnl_nmrc265_94_"
nmrc_url_tail = "_long.csv"

alpha_url_head = "http://www.nber.org/hcris/265-94/rnl_alpha265_94_"
alpha_url_tail = "_long.csv"


for (year in start_year:end_year) {

  if (!file.exists(paste0("hcris_raw//rpt_", as.character(year), ".csv"))) {
    download.file(paste0(rpt_url_head, as.character(year), rpt_url_tail),
                  paste0("hcris_raw//rpt_", as.character(year), ".csv"))
  }
  
  if (!file.exists(paste0("hcris_raw//nmrc_", as.character(year), ".csv"))) {
    download.file(paste0(nmrc_url_head, as.character(year), nmrc_url_tail),
                  paste0("hcris_raw//nmrc_", as.character(year), ".csv"))
  }
  
  if (!file.exists(paste0("hcris_raw//alpha_", as.character(year), ".csv"))) {
    download.file(paste0(alpha_url_head, as.character(year), alpha_url_tail),
                  paste0("hcris_raw//alpha_", as.character(year), ".csv"))
  }
}

#=============================================================================#
# Q1. Cleaning the Data                                                       #
#=============================================================================#

# Read in Downloaded Data

for (year in start_year:end_year) {
  assign(paste0("rpt_", as.character(year)), fread(
    paste0("hcris_raw//rpt_", as.character(year), ".csv")))
  
  assign(paste0("nmrc_", as.character(year)), fread(
    paste0("hcris_raw//nmrc_", as.character(year), ".csv")))
  
  assign(paste0("alpha_", as.character(year)), fread(
    paste0("hcris_raw//alpha_", as.character(year), ".csv")))
}

# Create Directory for Cleaned Data

if (!dir.exists("hcris_cleaned")) {
  dir.create("hcris_cleaned")
}

# Create lists to streamline loop below

year_vec   = start_year:end_year

nmrc_list  = list(nmrc_1998, nmrc_1999, nmrc_2000, nmrc_2001,
                  nmrc_2002, nmrc_2003, nmrc_2004, nmrc_2005,
                  nmrc_2006, nmrc_2007, nmrc_2008, nmrc_2009, 
                  nmrc_2010)

alpha_list = list(alpha_1998, alpha_1999, alpha_2000, alpha_2001,
                  alpha_2002, alpha_2003, alpha_2004, alpha_2005,
                  alpha_2006, alpha_2007, alpha_2008, alpha_2009, 
                  alpha_2010)

rpt_list   = list(rpt_1998, rpt_1999, rpt_2000, rpt_2001,
                  rpt_2002, rpt_2003, rpt_2004, rpt_2005,
                  rpt_2006, rpt_2007, rpt_2008, rpt_2009, 
                  rpt_2010)

# Widen & Merge nmrc and alpha data
# Merge previous with rpt data
# Transmute vars & reorder
# Save as hcris_YEAR.csv
# Combine all sets as hcris_data & save

if (length(year_vec) == length(nmrc_list) &
    length(year_vec) == length(alpha_list) & 
    length(year_vec) == length(rpt_list)) {
  
  hcris_data = c()
  
  for (i in 1:length(year_vec)) {
    
    if (!file.exists(paste0("hcris_cleaned//hcris_", year_vec[i], ".csv"))) {
      
      nmrc_wide = as.data.frame(nmrc_list[i]) %>%
        mutate(key_val = paste0(
          wksht_cd, 
          str_pad(line_num, 5, side = "left", pad = "0"),
          str_pad(clmn_num, 4, side = "left", pad = "0"))) %>%
        select(-wksht_cd, -line_num, -clmn_num) %>% 
        pivot_wider(names_from = key_val, values_from = itm_val_num)
      
      alpha_wide = as.data.frame(alpha_list[i]) %>%
        mutate(key_val = paste0(
          wksht_cd, 
          str_pad(line_num, 5, side = "left", pad = "0"),
          str_pad(clmn_num, 4, side = "left", pad = "0"))) %>%
        select(-wksht_cd, -line_num, -clmn_num) %>%
        pivot_wider(names_from = key_val, values_from = alphnmrc_itm_txt)
      
      nmrc_alpha_combined = inner_join(nmrc_wide, alpha_wide, by = "rpt_rec_num")
      
      combined_data = inner_join(as.data.frame(rpt_list[i]),
                                nmrc_alpha_combined, by = "rpt_rec_num")
      
      if ("A000000023000600" %in% colnames(combined_data)) {
        combined_data = combined_data %>%
          mutate(epo_cost = A000000023000600)
      } else {
        combined_data = combined_data %>%
          mutate(epo_cost = NA)
      }
      
      if ("S000001001020200" %in% colnames(combined_data)) {
        combined_data = combined_data %>%
          mutate(state = S000001001020200)
      } else {
        combined_data = combined_data %>%
          mutate(state = NA)
      }
      
      filtered_data = combined_data %>%
        transmute(
          report_number = rpt_rec_num,
          year = year_vec[i],
          facility_name = S000001001000100,
          non_medicare_sessions = S100000001000100,
          non_medicare_sessions_indirect = S100000002000100,
          avg_weekly_sessions = S100000004000100,
          avg_days_open_per_week = S100000005000100,
          avg_session_time = S100000006000100,
          num_machines_regular = S100000007000100,
          num_machines_standby = S100000008000100,
          dialyzer_type = S100000012000100,
          dialyser_reuse_times = S100000012000200,
          epo_total = S100000014000100,
          epo_cost = epo_cost,
          epo_rebates = A200000015000200,
          epo_net_cost = A200000016000200,
          chain_indicator = S000001009000100,
          chain_identity = S000001009010100,
          prvdr_num = S000001002000100,
          ever_hospital_based = S000001008000100,
          certification_date = S000001003000100,
          report_start_date = S000001005000100,
          report_end_date = S000001005000200,
          total_treatments_hd = C000010001000100,
          total_costs_hd_housekeeping = B000000007000200,
          total_costs_hd_machines = B000000007000300,
          total_costs_hd_salaries = B000000007000400,
          total_costs_hd_benefits = B000000007000500,
          total_costs_hd_drugs = B000000007000600,
          total_costs_hd_supplies = B000000007000700,
          total_costs_hd_labs = B000000007000800,
          total_costs_hd_other = B000000007001000,
          supplies = B000000003000700,
          lab_services = B000000004000800,
          total_treatments_pd = C000010002000100,
          state = state,
          zip_code = S000001001020300,
          fy_bgn_dt = fy_bgn_dt,
          fy_end_dt = fy_end_dt) %>%
        select(report_number, avg_days_open_per_week, avg_session_time,
               avg_weekly_sessions, dialyser_reuse_times, dialyzer_type,
               epo_net_cost, epo_rebates, epo_total, lab_services, 
               non_medicare_sessions,	non_medicare_sessions_indirect,
               num_machines_regular,	num_machines_standby,	supplies,
               total_costs_hd_benefits,	total_costs_hd_drugs,
               total_costs_hd_housekeeping,	total_costs_hd_labs,
               total_costs_hd_machines,	total_costs_hd_other,
               total_costs_hd_salaries,	total_costs_hd_supplies,
               total_treatments_hd,	total_treatments_pd,	certification_date,
               chain_identity,	chain_indicator,	ever_hospital_based,
               facility_name,	prvdr_num,	report_end_date,	report_start_date,
               zip_code,	fy_bgn_dt,	fy_end_dt,	year,	epo_cost, state)
      
      filtered_data %>%
        write_csv(paste0("hcris_cleaned//hcris_", 
                         year_vec[i], ".csv"), na = "")
      
      
      assign(paste0("hcris_", year_vec[i]), 
             filtered_data)
      
    } else {
      
      assign(paste0("hcris_", year_vec[i]), 
             fread(paste0("hcris_cleaned//hcris_", 
                    as.character(year_vec[i]), ".csv")))
      
    }
    
  }
  
} else(stop("Lists of Unequal Length"))

# Combine Into hcris_data & Save hcris_data.csv

if (!file.exists(paste0("hcris_cleaned//hcris_data.csv"))) {
  
  assign("hcris_data", fread(paste0("hcris_cleaned//hcris_", 
                                    as.character(year_vec[1]), ".csv")))
  
  for (i in 2:length(year_vec)) {
    
    temp_data = fread(paste0("hcris_cleaned//hcris_", 
                             as.character(year_vec[i]), ".csv")) 
    
    hcris_data = rbind(hcris_data, temp_data)
    
  }
  
  write_csv(hcris_data, "hcris_cleaned//hcris_data.csv",
            na = "")
  
} else {
  
  hcris_data = fread("hcris_cleaned//hcris_data.csv")
  
}

#1. Drops any observations where prvdr_num is missing
cleaned_data = hcris_data %>% 
  drop_na(prvdr_num)

#2. Takes the absolute values of negative epo_cost data
for (i in 1:nrow(cleaned_data)){
  if(cleaned_data[i, 38] < 0){
    cleaned_data[i, 38] = abs(cleaned_data[i, 38])
  }
}

#3. Replace missing epo_rebates values with zero
cleaned_data$epo_rebates[is.na(cleaned_data$epo_rebates)] = 0

#4
#epo cost is missing, epo net cost is not, and epo rebates equals 0
cleaned_data_4a = cleaned_data %>% 
  subset(is.na(epo_cost)) %>% 
  subset(!is.na(epo_net_cost)) %>% 
  subset(epo_rebates == 0) %>% 
  mutate(epo_cost = epo_net_cost)

#epo cost is missing, epo net cost is not, and epo rebates does not equal
cleaned_data_4b = cleaned_data %>% 
  subset(is.na(epo_cost)) %>% 
  subset(!is.na(epo_net_cost)) %>% 
  subset(epo_rebates != 0) %>% 
  mutate(epo_cost = epo_net_cost+epo_rebates)

#epo cost is missing, epo net cost is also missing, epo rebates is 0
cleaned_data_4c = cleaned_data %>% 
  subset(is.na(epo_cost)) %>% 
  subset(is.na(epo_net_cost)) %>%
  subset(epo_rebates == 0) %>% 
  mutate(epo_cost = 0) %>% 
  mutate(epo_net_cost = 0)

#epo cost is missing,epo net cost is missing, and epo rebates is not zero.
cleaned_data_4d = cleaned_data %>% 
  subset(is.na(epo_cost)) %>% 
  subset(is.na(epo_net_cost)) %>% 
  subset(epo_rebates != 0)
  
#epo cost is not missing and epo net cost is missing
cleaned_data_4e = cleaned_data %>% 
  subset(!is.na(epo_cost)) %>% 
  subset(is.na(epo_net_cost))

cleaned_data_4f = cleaned_data %>% 
  subset(!is.na(epo_cost)) %>% 
  subset(!is.na(epo_net_cost)) %>% 
  subset(!is.na(epo_rebates))

cleaned_data = rbind(cleaned_data_4a, cleaned_data_4b, cleaned_data_4c, cleaned_data_4e, cleaned_data_4f)


#5
cleaned_data = cleaned_data %>% 
  group_by(epo_net_cost > epo_cost) %>% 
  mutate(epo_cost_1 = epo_cost) %>% 
  mutate(epo_cost = epo_net_cost) %>% 
  mutate(epo_net_cost = epo_cost_1) %>% 
  select(-epo_cost_1) 

#6
cleaned_data %>% 
  group_by(prvdr_num == 322664) %>% 
  mutate(prvdr_num = 342664)

#7
cleaned_data$fy_bgn_dt = mdy(cleaned_data$fy_bgn_dt)
cleaned_data$fy_end_dt = mdy(cleaned_data$fy_end_dt)
cleaned_data$report_start_date = mdy(cleaned_data$report_start_date)
cleaned_data$report_end_date = mdy(cleaned_data$report_end_date)

#8, removes report_start_data and report_end_date and any obs from Puerto Rico
cleaned_data = cleaned_data %>% 
  select(-report_start_date) %>% 
  select(-report_end_date) %>% 
  subset(state != 'PR')

#9
#a
cleaned_data$zip_code = trimws(cleaned_data$zip_code)
cleaned_data$zip_code = substr(cleaned_data$zip_code, 1, 5)
cleaned_data$zip_code = as.numeric(cleaned_data$zip_code)

#b
# group by provider number, find the mode (most common) zip code between the provider numbers, map provider numbers to the zip codes most common to the same provider number
mode <- function(zip){
  which.max(tabulate(zip))
}

zip_dict = cleaned_data %>% 
  group_by(prvdr_num) %>% 
  summarise(
    common_zip_code = mode(zip_code)
  )

for ( i in 1:nrow(cleaned_data)){
  zip_index = match(cleaned_data$prvdr_num[i], zip_dict$prvdr_num)
  
  cleaned_data$zip_code[i] <- zip_dict$common_zip_code[zip_index]
}


#10

## Download source file, unzip and extract into table
ZipCodeSourceFile = "http://download.geonames.org/export/zip/US.zip"
temp <- tempfile()
download.file(ZipCodeSourceFile , temp)
ZipCodes <- read.table(unz(temp, "US.txt"), sep="\t")
unlink(temp)
names(ZipCodes) = c("CountryCode", "zip", "PlaceName", 
                    "AdminName1", "AdminCode1", "AdminName2", "AdminCode2", 
                    "AdminName3", "AdminCode3", "latitude", "longitude", "accuracy")


newZipCodes = select(ZipCodes, "zip", "AdminCode1")
names(newZipCodes)[1] <- "zip_code"
names(newZipCodes)[2] <- "state"

## merge cleaned data set and zip codes data set
merged_zcs = merge(cleaned_data, newZipCodes, by = "zip_code", all.x = T)

## Clean & fill in most of the missing data
for (i in 1:nrow(merged_zcs)) {
  if ( is.na(merged_zcs$state.y[i]) && (!is.na(merged_zcs$state.x[i])) ){
    merged_zcs$state.y[i] = merged_zcs$state.x[i]
  }
  
  if(merged_zcs$prvdr_num[i] == 142535) {
    merged_zcs$state.y[i] = "IL"
  }
  
  if(merged_zcs$prvdr_num[i] %in% c(222529:222529, 212529:212571)) {
    merged_zcs$state.y[i] = "MD"
  }
  
  if(grepl("LOUISVILLE", merged_zcs$chain_identity[i])){
    merged_zcs$state.y[i] = "KY"
  }
  
  if(grepl("BAYAMON", merged_zcs$chain_identity[i])){
    merged_zcs$state.y[i] = "PR"
  }
}

## Un-comment this part  if you want to continue classifying each missing state
# merged_zcs %>% 
#  filter(
#    state.y == ""
#  )

cleaned_data = merged_zcs
cleaned_data = subset(cleaned_data, select = -c(state.x) )
names(cleaned_data)[ncol(cleaned_data)] <- "state"
cleaned_data

#  11

dfo = c()

for (i in 1:nrow(cleaned_data)) {
  dfo[i] = NA
}

cleaned_data$dfo = dfo

for (i in 1:nrow(cleaned_data)) {
  if ( grepl("DAVITA", cleaned_data$chain_identity[i], ignore.case = T) ){
    cleaned_data$dfo[i] = "DAVITA"
  } else if (grepl("FRESENIUS", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "FRESENIUS"
  }
}


## All misspellings
for (i in 1:nrow(cleaned_data)) {
  
  # DAVITA Misspellings
  if ( grepl("DVITA", cleaned_data$chain_identity[i], ignore.case = T) ){
    cleaned_data$dfo[i] = "DAVITA"
  } else if (grepl("FRESENIUS", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "FRESENIUS"
  } else if (grepl("VITA", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "DAVITA"
  } else if (grepl("DANITA", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "DAVITA"
  } else if (grepl("DACITA", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "DAVITA"
  } else if (grepl("DATIVA", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "DAVITA"   
  } else if (grepl("DAVIATA", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "DAVITA"   
    
    # FRESENIUS Misspellings
  }  else if (grepl("FRENESIUS", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "FRESENIUS"
  }  else if (grepl("FERSENIUS", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "FRESENIUS"
  } else if (grepl("FENIOUS", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "FRESENIUS"
  } else if (grepl("FRES", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "FRESENIUS"
  } else if (grepl("FESENIUS", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "FRESENIUS"
  } else if (grepl("FREN", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "FRESENIUS"
  } else if (grepl("FR4ESENIUS", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "FRESENIUS"
  } else if (grepl("FREENIUS", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "FRESENIUS"
  } else if (grepl("FREENIUS", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "FRESENIUS"
  } else if (grepl("FREINIUS", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "FRESENIUS"
  } else if (grepl("FREZENIUS", cleaned_data$chain_identity[i], ignore.case = T)){
    cleaned_data$dfo[i] = "FRESENIUS"
  } 
  
}

## Fill in NA's with "Other"
for (i in 1:nrow(cleaned_data)) {
  if(is.na(cleaned_data$dfo[i])) {
    cleaned_data$dfo[i] = "Other"
  }
}


## Compile 
chain_id = c()
for (i in 1:nrow(cleaned_data)) {
  if (cleaned_data$chain_indicator[i] == "N" && !is.na(cleaned_data$chain_indicator[i])) {
    chain_id[i] = 0
  } else if (cleaned_data$chain_indicator[i] == "Y" && cleaned_data$dfo == "Other" && !is.na(cleaned_data$chain_indicator[i])){
    chain_id[i] = 1
  } 
  
  if (cleaned_data$dfo[i] == "DAVITA") {
    chain_id[i] = 2
  } else if (cleaned_data$dfo[i] == "FRESENIUS") {
    chain_id[i] = 3
  }
}

cleaned_data$chain_id = chain_id

