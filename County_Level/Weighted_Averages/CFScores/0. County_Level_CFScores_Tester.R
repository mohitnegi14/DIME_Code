################## 
# MOHIT NEGI
# Last Updated : 16 June 2023
# Contact on : mohit.negi@studbocconi.it
################## 
# This script tries out for a random FIPS = 54033

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
library(RStata)
################## 

# CSV is really big, so just load in relevant columns.
relevant_columns <- c('cycle', 'bonica.cid', 'date', 'amount', 'contributor.zipcode', 'bonica.rid', 'recipient.party', 'contributor.cfscore')

# Note down the cycles we want
cycles <- c(2002, 2004, 2006, 2008, 2010, 2012, 2014)

for(i in 1:5) {
  
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
  
  # For now, we only want incumbents. So filter them out. Key idea : non-incumbents have alpha-numeric ICPSR.
  # But this applies only to candidates, not committees.
  contribs <- contribs[!(recipient.type == 'cand' & str_detect(ICPSR2, pattern = '[:alpha:]')),]
  
  # Cleaning up the zip codes, since ZIP+4 i.e. 9 digit ones are not available for matching with counties.
  contribs <- contribs[, contributor.zipcode := str_sub(str_trim(contributor.zipcode, side = 'both'),1, 5)]
  
  # Load in the zip to county matching dataset.
  zip_county_match <- fread(glue('{raw}/Zip-Codes-to-City-County-State-2020.csv'), select = c('zip', 'county', 'state'))
  zip_county_match <- zip_county_match[, zip := str_sub(str_trim(zip, side = 'both'), 1, 5)]
  
  # Do the matching according to the zip code.
  contribs <- merge.data.table(contribs, zip_county_match,
                               by.x = 'contributor.zipcode',
                               by.y = 'zip',
                               all.x = TRUE) %>% 
    filter(!(state %in% c('AA', 'AE', 'AP')) & county != '')
  
  # Keep only matched ones.
  contribs <- contribs[!is.na(county),]
  
  # Convert date string to week of year format, will later be matched to disaster data.
  contribs <- contribs[, date := as_date(date)]
  
  # Stay in relevant time frame.
  contribs <- contribs[date > as_date('2001-01-01') & date < as_date('2014-12-31'),]
  
  # Remove words like county and borough to improve matching performance.
  contribs <- contribs[, county := str_replace_all(county, pattern = (' County'), replacement = '') %>% 
                         str_replace_all(pattern = (' Borough'), replacement = '') %>% 
                         str_replace_all(pattern = (' Census Area'), replacement = '') %>% 
                         str_replace_all(pattern = ('Municipality of '), replacement = '') %>% 
                         str_replace_all(pattern = ('St '), replacement = 'St. ')]
  
  # Load in a dataset that has the corresponding FIPs to names.
  name_to_fips_dset <- fread(glue('{raw}/County_zip_dma_converter_fips.csv'))
  name_to_fips_dset <- name_to_fips_dset %>% 
    rename('CountyFIPS' = 'fips') %>% 
    rename('StateAbbr' = 'state') %>% 
    rename('CountyName' = 'name')
  
  # Clean the names.
  name_to_fips_dset$CountyName <- str_replace_all(name_to_fips_dset$CountyName, 
                                                  pattern = (' County'), replacement = '') %>% 
    str_replace_all(pattern = (' Borough'), replacement = '') %>% 
    str_replace_all(pattern = (' Census Area'), replacement = '') %>% 
    str_replace_all(pattern = ('Municipality of '), replacement = '') %>% 
    str_replace_all(pattern = (' Municipality'), replacement = '') %>% 
    str_replace_all(pattern = ('St '), replacement = 'St. ') %>% 
    str_replace_all(pattern = ('City and'), replacement = '') %>% 
    str_replace_all(pattern = ('City'), replacement = '')
  
  # We do the matching only for the unique county names assigned in the big dataset for speed.
  unclean_county_names <- contribs[,.(county, state)] %>% unique()
  
  # Here is the scheme for the fuzzy match.
  ## First, to each county name in Master, match to all counties in same state.
  ## Second, calculate string distances for that county with all counties in same state.
  ## Third, select the closest match, provided distance is less than 10.
  unclean_county_names <- merge.data.table(unclean_county_names, name_to_fips_dset,
                                           by.x = 'state',
                                           by.y = 'StateAbbr',
                                           all.x = TRUE,
                                           allow.cartesian = TRUE)
  
  unclean_county_names <- unclean_county_names[, fuzzy_score := stringdist(county, CountyName)]
  
  unclean_county_names_bestmatch <- unclean_county_names[, Best := min(fuzzy_score), by = county][Best == fuzzy_score & fuzzy_score < 10,][,.(state, county, CountyName, CountyFIPS, fuzzy_score)]
  setnames(unclean_county_names_bestmatch, 'CountyFIPS', 'FIPS')
  
  fips_matching_df <- unclean_county_names_bestmatch[, .(state, county, FIPS)]
  #
  
  contribs <- merge.data.table(contribs, fips_matching_df,
                               by.x = c('state', 'county'),
                               by.y = c('state', 'county'),
                               all.x = FALSE)
  
  # Keep only 2 randomly chosen counties : 18183, 37187.
  contribs <- contribs[FIPS %in% c(18183, 37187),]
  
  # Simple count tracker.
  contribs <- contribs[, count := 1] 
  
  # For now, dropping negative donations.
  contribs <- contribs[amount >= 0,]
  # contribs$count[which(contribs$amount < 0)] <- -1
  
  contribs <- contribs[, `:=`(cfscore_avail = !(is.na(contributor.cfscore)),
                              cfscore_pos = contributor.cfscore >= 0,
                              cfscore_neg = contributor.cfscore < 0,
                              year = year(date))]
  
  # Now collapse individual data to get data only on the county-week-recipient level.
  contribs <- contribs[, .(TPD = sum(amount),
                           Donation_Count = sum(count),
                           TPD_cf = sum(amount * cfscore_avail),
                           Donation_Count_cf = sum(count * cfscore_avail),
                           TPD_cfpos = sum(amount * cfscore_pos),
                           Donation_Count_cfpos = sum(count * cfscore_pos),
                           TPD_cfneg = sum(amount * cfscore_neg),
                           Donation_Count_cfneg = sum(count * cfscore_neg)),
                       by = .(FIPS, ICPSR2, date, recipient.type)]
  
  # Load in green PAC data.
  # ICPSR column must be named 'ICPSR'.
  green_PACs <- fread(glue('{cleaned}/greenPACs_icpsr.csv'))
  green_PACs <- unlist(green_PACs, use.names=FALSE)
  green_PACs <- green_PACs[which(green_PACs != '')] %>% unique()
  green_PACs <- data.table('ICPSR' = green_PACs)
  
  # Create indicators for whether the recipient was a candidate or a PAC.
  contribs <- contribs[, `:=`(indicator_cand = (recipient.type == 'cand'),
                              indicator_PAC = (recipient.type == 'comm'))]
  
  # Create indicator if the recipient is a green PAC.
  contribs <- contribs[, indicator_greenPAC := ((contribs$ICPSR2 %in% unique(green_PACs$ICPSR)) * indicator_PAC)]
  
  # Now load in LCV scorecard data.
  LCV_scores <- fread(glue('{cleaned}/harmonized_scorecards.csv'))
  LCV_scores$ICPSR <- as.character(LCV_scores$ICPSR)
  
  # Ed Markey - middle of 2013 rep to sen anomaly, appears twice. Remove his senate record. (14435)
  # Steven Kirk - Nov 2010 transition (20115)
  # Dean Heller - May 2011, keep senate now. (20730)
  LCV_scores <- LCV_scores[!(ICPSR == 14435 & Year == 2013 & Chamber == 2),]
  LCV_scores <- LCV_scores[!(ICPSR == 20115 & Year == 2010 & Chamber == 2),]
  LCV_scores <- LCV_scores[!(ICPSR == 20730 & Year == 2011 & Chamber == 1),]
  
  LCV_indicator_dset <- LCV_scores[,.(ICPSR, Year, Nominal.Score, Adj.Score, indicator_has_LCV_data = 1)]
  
  # # We want the scores from the year before.
  contribs <- contribs[, prev_year := (year(date) - 1)]
  
  # So now, the indicator is 1 only if in the previous year, the receiver had a LCV score (i.e. was in the LCV scores dataset).
  contribs <- merge.data.table(contribs, LCV_indicator_dset,
                               by.x = c('ICPSR2', 'prev_year'),
                               by.y = c('ICPSR', 'Year'),
                               all.x = TRUE)
  
  #### Make the final columns for analysis. We will only consider donations to LCV data candidates now.
  contribs <- contribs[, `:=`(TPD_cands = (TPD * indicator_cand * indicator_has_LCV_data),
                              Donation_count_cands = (Donation_Count * indicator_cand * indicator_has_LCV_data),
                              TPD_cands_cf = (TPD_cf * indicator_cand * indicator_has_LCV_data),
                              Donation_count_cands_cf = (Donation_Count_cf * indicator_cand * indicator_has_LCV_data),
                              TPD_cands_cfpos = (TPD_cfpos * indicator_cand * indicator_has_LCV_data),
                              Donation_count_cands_cfpos = (Donation_Count_cfpos * indicator_cand * indicator_has_LCV_data),
                              TPD_cands_cfneg = (TPD_cfneg * indicator_cand * indicator_has_LCV_data),
                              Donation_count_cands_cfneg = (Donation_Count_cfneg * indicator_cand * indicator_has_LCV_data))]
  
  contribs <- contribs[, `:=`(wsum_amounts_adj = (TPD_cands * Adj.Score),
                              wsum_amounts_nominal = (TPD_cands * Nominal.Score),
                              wsum_counts_adj = (Donation_count_cands * Adj.Score),
                              wsum_counts_nominal = (Donation_count_cands * Nominal.Score),
                              wsum_amounts_adj_cfpos = (TPD_cands_cfpos * Adj.Score),
                              wsum_amounts_nominal_cfpos = (TPD_cands_cfpos * Nominal.Score),
                              wsum_counts_adj_cfpos = (Donation_count_cands_cfpos * Adj.Score),
                              wsum_counts_nominal_cfpos = (Donation_count_cands_cfpos * Nominal.Score),
                              wsum_amounts_adj_cfneg = (TPD_cands_cfneg * Adj.Score),
                              wsum_amounts_nominal_cfneg = (TPD_cands_cfneg * Nominal.Score),
                              wsum_counts_adj_cfneg = (Donation_count_cands_cfneg * Adj.Score),
                              wsum_counts_nominal_cfneg = (Donation_count_cands_cfneg * Nominal.Score))]
  
  # Remove unimportant columns now.
  contribs <- contribs[, c('prev_year',
                           'recipient.type',
                           'TPD',
                           'Donation_Count',
                           'TPD_cf',
                           'Donation_Count_cf',
                           'TPD_cfpos',
                           'Donation_Count_cfpos',
                           'TPD_cfneg',
                           'Donation_Count_cfneg',
                           'indicator_cand',
                           'indicator_PAC',
                           'indicator_greenPAC',
                           'Nominal.Score',
                           'Adj.Score',
                           'indicator_has_LCV_data') := NULL]
  
  # Since we are, for now, only considering donations to candidates, many NAs (when the donation was to a PAC),
  # So remove them.
  contribs <- contribs[!is.na(TPD_cands),]
  
  fwrite(contribs, glue('{cleaned}/test_contribs_cf_{cycles[i]}.csv'))
  
  rm(contribs)
  
}

#### NEXT STEP : COMBINE THESE DATA INTO ONE.
list_of_datasets_by_year <- list.files(path = glue('{cleaned}/'), pattern = 'test_contribs_cf_20*')

list_of_datasets_by_year

full_list <- list()
for(i in 1:length(list_of_datasets_by_year)) {

  contribs <- fread(glue('{cleaned}/{list_of_datasets_by_year[i]}'))
  full_list[[i]] <- contribs

}

full_df <- bind_rows(full_list)

# Write it as csv. Done.
fwrite(full_df, glue('{output}/test_contribs_cf_combined.csv'))

#########
#########
#########
#########
#########
# NEXT STEP, GO TO STATA, CONVERT DATES TO WEEKS, COME BACK HERE AND COLLAPSE TO COUNTY-WEEK LEVEL.

full_df <- fread(glue('{output}/test_contribs_cf_combined.csv'))

# Now collapse to county-week level, our final unit.
full_df <- full_df[, .(tpd_cands = sum(tpd_cands),
                       donation_count_cands = sum(donation_count_cands),
                       tpd_cands_cf = sum(tpd_cands_cf),
                       donation_count_cands_cf = sum(donation_count_cands_cf),
                       tpd_cands_cfpos = sum(tpd_cands_cfpos),
                       donation_count_cands_cfpos = sum(donation_count_cands_cfpos),
                       tpd_cands_cfneg = sum(tpd_cands_cfneg),
                       donation_count_cands_cfneg = sum(donation_count_cands_cfneg),
                       wsum_amounts_adj = sum(wsum_amounts_adj),
                       wsum_amounts_nominal = sum(wsum_amounts_nominal),
                       wsum_counts_adj = sum(wsum_counts_adj),
                       wsum_counts_nominal = sum(wsum_counts_nominal),
                       wsum_amounts_adj_cfpos = sum(wsum_amounts_adj_cfpos),
                       wsum_amounts_nominal_cfpos = sum(wsum_amounts_nominal_cfpos),
                       wsum_counts_adj_cfpos = sum(wsum_counts_adj_cfpos),
                       wsum_counts_nominal_cfpos = sum(wsum_counts_nominal_cfpos),
                       wsum_amounts_adj_cfneg = sum(wsum_amounts_adj_cfneg),
                       wsum_amounts_nominal_cfneg = sum(wsum_amounts_nominal_cfneg),
                       wsum_counts_adj_cfneg = sum(wsum_counts_adj_cfneg),
                       wsum_counts_nominal_cfneg = sum(wsum_counts_nominal_cfneg)),
                   by = .(fips, week)]

# Check
fw <- full_df[, .(fips, week)]
fw2 <- unique(fw)
# Yes.

fwrite(full_df, glue('{output}/test_contribs_cf_combined.csv'))

# NOW TAKE THIS BACK TO STATA.

#########
#########
#########
#########
#########
# To Test,
# FIPS 54033, 2008W21 : MAY19-25, 690TPDCANDS, 9COUNTS, 2BYPOS, 7BYNEG.

for(i in 1:5) {
  
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
  
  # For now, we only want incumbents. So filter them out. Key idea : non-incumbents have alpha-numeric ICPSR.
  # But this applies only to candidates, not committees.
  contribs <- contribs[!(recipient.type == 'cand' & str_detect(ICPSR2, pattern = '[:alpha:]')),]
  
  # Cleaning up the zip codes, since ZIP+4 i.e. 9 digit ones are not available for matching with counties.
  contribs <- contribs[, contributor.zipcode := str_sub(str_trim(contributor.zipcode, side = 'both'),1, 5)]
  
  # Load in the zip to county matching dataset.
  zip_county_match <- fread(glue('{raw}/Zip-Codes-to-City-County-State-2020.csv'), select = c('zip', 'county', 'state'))
  zip_county_match <- zip_county_match[, zip := str_sub(str_trim(zip, side = 'both'), 1, 5)]
  
  # Do the matching according to the zip code.
  contribs <- merge.data.table(contribs, zip_county_match,
                               by.x = 'contributor.zipcode',
                               by.y = 'zip',
                               all.x = TRUE) %>% 
    filter(!(state %in% c('AA', 'AE', 'AP')) & county != '')
  
  # Keep only matched ones.
  contribs <- contribs[!is.na(county),]
  
  # Convert date string to week of year format, will later be matched to disaster data.
  contribs <- contribs[, date := as_date(date)]
  
  # Stay in relevant time frame.
  contribs <- contribs[date > as_date('2001-01-01') & date < as_date('2014-12-31'),]
  
  # Remove words like county and borough to improve matching performance.
  contribs <- contribs[, county := str_replace_all(county, pattern = (' County'), replacement = '') %>% 
                         str_replace_all(pattern = (' Borough'), replacement = '') %>% 
                         str_replace_all(pattern = (' Census Area'), replacement = '') %>% 
                         str_replace_all(pattern = ('Municipality of '), replacement = '') %>% 
                         str_replace_all(pattern = ('St '), replacement = 'St. ')]
  
  # Load in a dataset that has the corresponding FIPs to names.
  name_to_fips_dset <- fread(glue('{raw}/County_zip_dma_converter_fips.csv'))
  name_to_fips_dset <- name_to_fips_dset %>% 
    rename('CountyFIPS' = 'fips') %>% 
    rename('StateAbbr' = 'state') %>% 
    rename('CountyName' = 'name')
  
  # Clean the names.
  name_to_fips_dset$CountyName <- str_replace_all(name_to_fips_dset$CountyName, 
                                                  pattern = (' County'), replacement = '') %>% 
    str_replace_all(pattern = (' Borough'), replacement = '') %>% 
    str_replace_all(pattern = (' Census Area'), replacement = '') %>% 
    str_replace_all(pattern = ('Municipality of '), replacement = '') %>% 
    str_replace_all(pattern = (' Municipality'), replacement = '') %>% 
    str_replace_all(pattern = ('St '), replacement = 'St. ') %>% 
    str_replace_all(pattern = ('City and'), replacement = '') %>% 
    str_replace_all(pattern = ('City'), replacement = '')
  
  # We do the matching only for the unique county names assigned in the big dataset for speed.
  unclean_county_names <- contribs[,.(county, state)] %>% unique()
  
  # Here is the scheme for the fuzzy match.
  ## First, to each county name in Master, match to all counties in same state.
  ## Second, calculate string distances for that county with all counties in same state.
  ## Third, select the closest match, provided distance is less than 10.
  unclean_county_names <- merge.data.table(unclean_county_names, name_to_fips_dset,
                                           by.x = 'state',
                                           by.y = 'StateAbbr',
                                           all.x = TRUE,
                                           allow.cartesian = TRUE)
  
  unclean_county_names <- unclean_county_names[, fuzzy_score := stringdist(county, CountyName)]
  
  unclean_county_names_bestmatch <- unclean_county_names[, Best := min(fuzzy_score), by = county][Best == fuzzy_score & fuzzy_score < 10,][,.(state, county, CountyName, CountyFIPS, fuzzy_score)]
  setnames(unclean_county_names_bestmatch, 'CountyFIPS', 'FIPS')
  
  fips_matching_df <- unclean_county_names_bestmatch[, .(state, county, FIPS)]
  #
  
  contribs <- merge.data.table(contribs, fips_matching_df,
                               by.x = c('state', 'county'),
                               by.y = c('state', 'county'),
                               all.x = FALSE)
  
  # Keep only 2 randomly chosen counties : 18183, 37187.
  contribs <- contribs[FIPS %in% c(18183, 37187),]
  
  # Simple count tracker.
  contribs <- contribs[, count := 1] 
  
  # For now, dropping negative donations.
  contribs <- contribs[amount >= 0,]
  
  fwrite(contribs, glue('{cleaned}/test_check_contribs_cf_{cycles[i]}.csv'))
  
}

list_of_datasets_by_year <- list.files(path = glue('{cleaned}/'), pattern = 'test_check_contribs_cf_20*')

list_of_datasets_by_year

full_list <- list()
for(i in 1:length(list_of_datasets_by_year)) {
  
  contribs <- fread(glue('{cleaned}/{list_of_datasets_by_year[i]}'))
  full_list[[i]] <- contribs
  
}

full_df <- bind_rows(full_list)

# REMEMBER, To Test,
# FIPS : 18183 & 37187, 2008W10 : MARCH3-9 APPROX, TPDCANDS : 100 & 187.54, COUNTS : 1 & 2, BYPOS : 0 & 0, BYNEG : 1 & 2.

test <- full_df[date > date('2008/03/01') & date < date('2008/04/01') & recipient.type == 'cand']
test <- test[, week1 := week(date)]

# Works out. Note that here it aligned perfectly since receivers had LCV scores.
# test doesn't include donations to non-LCV receivers and test_check does, so account for that.
