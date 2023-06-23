################## 
# MOHIT NEGI
# Last Updated : 16 June 2023
# Contact on : mohit.negi@studbocconi.it
################## 
# This script tries out for 2 random zips.

################## PATHS
harddrive <- 'D:/Mohit_Work/DIME/ContribDB'
localfiles <- 'C:/Users/anjun/OneDrive/Desktop/EP/DIME/ContribDB'
raw <- 'C:/Users/anjun/OneDrive/Desktop/EP/DIME/DIME_Data/Raw'
cleaned <- 'C:/Users/anjun/OneDrive/Desktop/EP/DIME/DIME_Data/Cleaned'
output <- 'C:/Users/anjun/OneDrive/Desktop/EP/DIME/DIME_Data/Output/ZIP_Level/Weighted_Averages'
##################

################## LIBRARIES
library(tidyverse)
library(data.table)
library(stringr)
library(glue)
library(stringdist)
library(fuzzyjoin)
library(lubridate)
################## 

# CSV is really big, so just load in relevant columns.
relevant_columns <- c('cycle', 'bonica.cid', 'date', 'amount', 'contributor.zipcode', 'bonica.rid', 'recipient.party')

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
  ###########################################################################
  # Keep only 2 randomly chosen zips : 28217, 94558.
  
  sample_zips <- c('29650')
  
  contribs <- contribs[contributor.zipcode %in% sample_zips,]
  
  # Simple count tracker.
  contribs <- contribs[, count := 1] 
  
  # For now, dropping negative donations.
  contribs <- contribs[amount >= 0,]
  # contribs$count[which(contribs$amount < 0)] <- -1
  
  contribs <- contribs[, `:=`(above200 = (amount > 200),
                              below200 = (amount <= 200))]
  contribs <- contribs[, `:=`(above200_amount = (above200 * amount),
                              below200_amount = (below200 * amount))]
  
  # Now collapse individual data to get data only on the zip-date-recipient level.
  contribs <- contribs[, .(TPD = sum(amount), 
                           Donation_count = sum(count), 
                           TPD_above200 = sum(above200_amount),
                           TPD_below200 = sum(below200_amount),
                           Donation_count_above200 = sum(above200),
                           Donation_count_below200 = sum(below200),
                           FIPS = first(FIPS),
                           Party = first(recipient.party)),
                       by = .(contributor.zipcode, ICPSR2, date, recipient.type)]
  
  # Load in green PAC data.
  # ICPSR column must be named 'ICPSR'.
  green_PACs <- fread(glue('{cleaned}/greenPACs_icpsr.csv'))
  green_PACs <- unlist(green_PACs, use.names=FALSE)
  green_PACs <- green_PACs[which(green_PACs != '')] %>% unique()
  green_PACs <- data.table('ICPSR' = green_PACs)
  
  # Not needed for Test.
  # contribs1 <- contribs[, .(contributor.zipcode, ICPSR2, date, TPD, Donation_count, Party)]
  # fwrite(contribs1, glue('{cleaned}/zip_partisanship_{cycles[i]}.csv'))
  
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
                              Donation_count_cands = (Donation_count * indicator_cand * indicator_has_LCV_data),
                              TPD_cands_above200 = (TPD_above200 * indicator_cand * indicator_has_LCV_data),
                              TPD_cands_below200 = (TPD_below200 * indicator_cand * indicator_has_LCV_data),
                              Donation_count_cands_above200 = (Donation_count_above200 * indicator_cand * indicator_has_LCV_data),
                              Donation_count_cands_below200 = (Donation_count_below200 * indicator_cand * indicator_has_LCV_data))]
  
  contribs <- contribs[, `:=`(wsum_amounts_adj = (TPD_cands * Adj.Score),
                              wsum_amounts_nominal = (TPD_cands * Nominal.Score),
                              wsum_counts_adj = (Donation_count_cands * Adj.Score),
                              wsum_counts_nominal = (Donation_count_cands * Nominal.Score),
                              wsum_amounts_adj_above200 = (TPD_cands_above200 * Adj.Score),
                              wsum_amounts_nominal_above200 = (TPD_cands_above200 * Nominal.Score),
                              wsum_counts_adj_above200 = (Donation_count_cands_above200 * Adj.Score),
                              wsum_counts_nominal_above200 = (Donation_count_cands_above200 * Nominal.Score),
                              wsum_amounts_adj_below200 = (TPD_cands_below200 * Adj.Score),
                              wsum_amounts_nominal_below200 = (TPD_cands_below200 * Nominal.Score),
                              wsum_counts_adj_below200 = (Donation_count_cands_below200 * Adj.Score),
                              wsum_counts_nominal_below200 = (Donation_count_cands_below200 * Nominal.Score))]
  
  # Remove unimportant columns now.
  contribs <- contribs[, c('prev_year',
                           'recipient.type',
                           'TPD',
                           'Donation_count',
                           'TPD_above200',
                           'Donation_count_above200',
                           'TPD_below200',
                           'Donation_count_below200',
                           'indicator_cand',
                           'indicator_PAC',
                           'indicator_greenPAC',
                           'Nominal.Score',
                           'Adj.Score',
                           'indicator_has_LCV_data') := NULL]
  
  # Since we are, for now, only considering donations to candidates, many NAs (when the donation was to a PAC),
  # So remove them.
  contribs <- contribs[!is.na(TPD_cands),]
  
  fwrite(contribs, glue('{cleaned}/testnewzip_contribs_{cycles[i]}.csv'))
  
  rm(contribs)
  
}

#### NEXT STEP : COMBINE THESE DATA INTO ONE.
list_of_datasets_by_year <- list.files(path = glue('{cleaned}/'), pattern = 'testnewzip_contribs_20*')

list_of_datasets_by_year

full_list <- list()
for(i in 1:length(list_of_datasets_by_year)) {

  contribs <- fread(glue('{cleaned}/{list_of_datasets_by_year[i]}'))
  contribs <- contribs[, Party := as.character(Party)]
  full_list[[i]] <- contribs

}

full_df <- bind_rows(full_list)

# Write it as csv. Done.
fwrite(full_df, glue('{output}/testnewzip_contribs_combined.csv'))

#########
#########
#########
#########
#########
# NEXT STEP, GO TO STATA, CONVERT DATES TO WEEKS, COME BACK HERE AND COLLAPSE TO COUNTY-WEEK LEVEL.

full_df <- fread(glue('{output}/testnewzip_contribs_combined.csv'))

# Now collapse to county-week level, our final unit.
full_df <- full_df[, .(tpd_cands = sum(tpd_cands),
                       donation_count_cands = sum(donation_count_cands),
                       tpd_cands_above200 = sum(tpd_cands_above200),
                       donation_count_cands_above200 = sum(donation_count_cands_above200),
                       tpd_cands_below200 = sum(tpd_cands_below200),
                       donation_count_cands_below200 = sum(donation_count_cands_below200),
                       wsum_amounts_adj = sum(wsum_amounts_adj),
                       wsum_amounts_nominal = sum(wsum_amounts_nominal),
                       wsum_counts_adj = sum(wsum_counts_adj),
                       wsum_counts_nominal = sum(wsum_counts_nominal),
                       wsum_amounts_adj_above200 = sum(wsum_amounts_adj_above200),
                       wsum_amounts_nominal_above200 = sum(wsum_amounts_nominal_above200),
                       wsum_counts_adj_above200 = sum(wsum_counts_adj_above200),
                       wsum_counts_nominal_above200 = sum(wsum_counts_nominal_above200),
                       wsum_amounts_adj_below200 = sum(wsum_amounts_adj_below200),
                       wsum_amounts_nominal_below200 = sum(wsum_amounts_nominal_below200),
                       wsum_counts_adj_below200 = sum(wsum_counts_adj_below200),
                       wsum_counts_nominal_below200 = sum(wsum_counts_nominal_below200),
                       fips = first(fips)),
                   by = .(contributorzipcode, week)]

# Check
fw <- full_df[, .(contributorzipcode, week)]
fw2 <- unique(fw)
# Yes.

fwrite(full_df, glue('{output}/testnewzip_contribs_combined.csv'))

# library(haven)
# FEMA_data <- read_dta(glue('{raw}/FEMA_disaster_county_clean.dta'))
# zip_to_fips <- read_dta(glue('{cleaned}/zip_to_fips.dta'))
# zip_to_fips$fipscode <- as.character(zip_to_fips$fips)
# 
# library(tidyverse)
# df <- left_join(FEMA_data,
#                 zip_to_fips,
#                 by = 'fipscode')
# 
# write_dta(df, glue('{raw}/FEMA_disaster_county_clean_zips.dta'))

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
  ###########################################################################
  # Keep only 2 randomly chosen zips : 28217, 94558.
  
  sample_zips <- c('28217', '94558')
  
  contribs <- contribs[contributor.zipcode %in% sample_zips,]
  
  # Simple count tracker.
  contribs <- contribs[, count := 1] 
  
  # For now, dropping negative donations.
  contribs <- contribs[amount >= 0,]
  
  fwrite(contribs, glue('{cleaned}/testnewcheck_zip_contribs_{cycles[i]}.csv'))
  
}

list_of_datasets_by_year <- list.files(path = glue('{cleaned}/'), pattern = 'testnewcheck_zip_contribs_20*')

list_of_datasets_by_year

full_list <- list()
for(i in 1:length(list_of_datasets_by_year)) {
  
  contribs <- fread(glue('{cleaned}/{list_of_datasets_by_year[i]}'))
  full_list[[i]] <- contribs
  
}

full_df <- bind_rows(full_list)

# REMEMBER, To Test,
# ZIP : 28217 & 94558, 2010W20 : MAY17-23 APPROX, TPDCANDS : 2000 & 5600, COUNTS : 1 & 9, BIG : 2000(1) & 5300(7), SMALL : 0 & 300(2).

test <- full_df[date > date('2010/05/01') & date < date('2010/06/01') & recipient.type == 'cand']
test <- test[, week1 := week(date)]

# Works out. Note that here it aligned perfectly since receivers had LCV scores.
# test doesn't include donations to non-LCV receivers and testnewcheck does, so account for that.

# DISPARITY FOR ZIP 94558, BUT EXPLAINABLE.
#6100(13) : ICPSR = 29502 NO LCV = 50(1), 21186 NO LCV = 250(1), 41104 NO LCV = 100(1), 21304 NO LCV = 100(1)
# TOTAL TO REMOVE = 500(4)
# SO NOW, 5600(9) WHICH IS CORRECT.







cc <- contribs

cc <- cc[recipient.type == 'cand',]
