# Testing for a particular county.
# STEPS
# 1. Select a random county - 31153 Sarpy County
# 2. From zip-codes.com, Manually find all the zips in this county - 
# 68005, 68028, 68046, 68056, 68059, 68113, 68123, 68128, 68133, 68136, 68138, 68147, 68157
# 3. Select a random week - 2008w27 (say)
# 4. Download raw DIME data, make the appropriate filterations (keep only cands, those with valid ICPSR id etc.)
# and keep only the above zips.
# 5. Compare the 2008w37 tpd etc. numbers in the final output file
# with the raw (filtered) data and ensure it matches.
# Note that the final output considers only donations to people
# with a LCV score in the year prior to the donation whereas the
# raw data will include everything - so keep it in mind.

################## PATHS
harddrive <- 'D:/Mohit_Work/DIME/ContribDB'
localfiles <- 'C:/Users/anjun/OneDrive/Desktop/EP/DIME/ContribDB'
raw <- 'C:/Users/anjun/OneDrive/Desktop/EP/DIME/DIME_Data/Raw'
cleaned <- 'C:/Users/anjun/OneDrive/Desktop/EP/DIME/DIME_Data/Cleaned'
output <- 'C:/Users/anjun/OneDrive/Desktop/EP/DIME/DIME_Data/Output/County_Level/Weighted_Averages/CFScores'
##################

################## LIBRARIES
library(tidyverse)
library(data.table)
library(stringr)
library(glue)
library(stringdist)
library(fuzzyjoin)
library(lubridate)
library(googledrive)
################## 

# CSV is really big, so just load in relevant columns.
relevant_columns <- c('cycle', 'bonica.cid', 'date', 'amount', 'contributor.zipcode', 'bonica.rid', 'recipient.party', 'contributor.cfscore')

# Note down the cycles we want
cycles <- c(2002, 2004, 2006, 2008, 2010, 2012, 2014)

for(i in 1:length(cycles)) {
  
  print(glue('We are on {cycles[i]}'))
  
  if(i < 6) {
    # Now load it in.
    contribs <- fread(glue('{localfiles}/contribDB_{cycles[i]}.csv'), select = relevant_columns)
    
  } else contribs <- fread(glue('{harddrive}/contribDB_{cycles[i]}.csv'), select = relevant_columns)
  
  # Now we want to get the recipients ICPSR id from what we have - bonica.rid. 
  # This is needed since LCV data has ICPSR ids. 
  # The following dataset has the matchings.
  recipients <- fread(glue('{raw}/dime_recipients_all_1979_2014.csv'), select = c('ICPSR2', 'bonica.rid', 'seat', 'recipient.type')) %>% 
    filter(seat %in% c('federal:senate', 'federal:house', 'federal:committee')) %>% 
    select(-seat) %>% 
    unique()
  
  # Match them.
  contribs <- merge.data.table(contribs, recipients,
                               by.x = 'bonica.rid',
                               by.y = 'bonica.rid',
                               all.x = TRUE,
                               allow.cartesian = TRUE)
  contribs <- contribs[!is.na(ICPSR2),]
  
  # Convert date string to week of year format, will later be matched to disaster data.
  contribs <- contribs[, date := as_date(date)]
  
  # Stay in relevant time frame.
  contribs <- contribs[date > as_date('2001-01-01') & date < as_date('2014-12-31'),]
  
  # For now, we only want incumbents. So filter them out. Key idea : non-incumbents have alpha-numeric ICPSR.
  # But this applies only to candidates, not committees.
  contribs <- contribs[!(recipient.type == 'cand' & str_detect(ICPSR2, pattern = '[:alpha:]')),]
  
  # Cleaning up the zip codes, since ZIP+4 i.e. 9 digit ones are not available for matching with counties.
  contribs <- contribs[, contributor.zipcode := str_sub(str_trim(contributor.zipcode, side = 'both'),1, 5)]
  
  contribs <- contribs[contributor.zipcode %in% c('68005', '68028', '68046', '68056', '68059', '68113', '68123', '68128', '68133', '68136', '68138', '68147', '68157'),]
  
  # Simple count tracker.
  contribs <- contribs[, count := 1] 
  
  # For now, dropping negative donations.
  contribs <- contribs[amount >= 0,]
  
  contribs <- contribs[, `:=`(cfscore_avail = !(is.na(contributor.cfscore)),
                              cfscore_pos = contributor.cfscore >= 0,
                              cfscore_neg = contributor.cfscore < 0,
                              year = year(date))]
  
  fwrite(contribs, glue('{cleaned}/test1_contribs_cf_{cycles[i]}.csv'))
  
  rm(contribs)
  
}

#### NEXT STEP : COMBINE THESE DATA INTO ONE.
list_of_datasets_by_year <- list.files(path = glue('{cleaned}/'), pattern = 'test1_contribs_cf_20*')

list_of_datasets_by_year

full_list <- list()
for(i in 1:length(list_of_datasets_by_year)) {
  
  contribs <- fread(glue('{cleaned}/{list_of_datasets_by_year[i]}'))
  full_list[[i]] <- contribs
  
}

full_df <- bind_rows(full_list)

# Write it as csv. Done.
fwrite(full_df, glue('{output}/test1_contribs_cf_combined.csv'))

# Now load in LCV scorecard data.
LCV_scores <- fread(glue('{cleaned}/harmonized_scorecards.csv'))
LCV_scores$ICPSR <- as.character(LCV_scores$ICPSR)

# Ed Markey - middle of 2013 rep to sen anomaly, appears twice. Remove his senate record. (14435)
# Steven Kirk - Nov 2010 transition (20115)
# Dean Heller - May 2011, keep senate now. (20730)
LCV_scores <- LCV_scores[!(ICPSR == 14435 & Year == 2013 & Chamber == 2),]
LCV_scores <- LCV_scores[!(ICPSR == 20115 & Year == 2010 & Chamber == 2),]
LCV_scores <- LCV_scores[!(ICPSR == 20730 & Year == 2011 & Chamber == 1),]

# FINAL DSET ANSWER FOR TPD_CANDS = 5000.
test <- full_df[, week := week(date)]

test <- test[year == 2008 & week %in% c(27),]

# ones with LCV score are - 15039. That's it.
# 500 + 1000 + 1000 + 2300 + 50 + 100 + 50 = 5000.
# CORRECT!