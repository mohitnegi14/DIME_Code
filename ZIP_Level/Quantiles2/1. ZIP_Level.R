################## 
# MOHIT NEGI
# Last Updated : 23 June 2023
# Contact on : mohit.negi@studbocconi.it
################## 

################## PATHS
harddrive <- 'D:/Mohit_Work/DIME/ContribDB'
localfiles <- 'C:/Users/anjun/OneDrive/Desktop/EP/DIME/ContribDB'
raw <- 'C:/Users/anjun/OneDrive/Desktop/EP/DIME/DIME_Data/Raw'
cleaned <- 'C:/Users/anjun/OneDrive/Desktop/EP/DIME/DIME_Data/Cleaned'
output <- 'C:/Users/anjun/OneDrive/Desktop/EP/DIME/DIME_Data/Output/ZIP_Level/Quantiles2'
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
cycles <- c(1998, 2000, 2002, 2004, 2006, 2008, 2010, 2012, 2014)

for(i in 3:length(cycles)) {
  
  print(glue('We are on {cycles[i]}'))
  
  if(i %in% c(3,4,5,6,7)) {
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
  if(i > 2) {
  contribs <- contribs[date > as_date('2001-01-01') & date < as_date('2014-12-31'),]
  } else contribs <- contribs[date > as_date('1996-01-01') & date < as_date('2014-12-31'),]
  
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
  contribs <- contribs[, .(TPD = sum(amount, na.rm = TRUE), 
                           Donation_count = sum(count, na.rm = TRUE), 
                           TPD_above200 = sum(above200_amount, na.rm = TRUE),
                           TPD_below200 = sum(below200_amount, na.rm = TRUE),
                           Donation_count_above200 = sum(above200, na.rm = TRUE),
                           Donation_count_below200 = sum(below200, na.rm = TRUE),
                           FIPS = first(FIPS),
                           Party = first(recipient.party)),
                       by = .(contributor.zipcode, ICPSR2, date, recipient.type)]
  
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
  contribs <- contribs[, indicator_greenPAC := ((ICPSR2 %in% unique(green_PACs$ICPSR)) * indicator_PAC)]
  
  # Now load in LCV scorecard data.
  LCV_scores <- fread(glue('{cleaned}/harmonized_scorecards.csv'))
  LCV_scores$ICPSR <- as.character(LCV_scores$ICPSR)
  
  LCV_scores <- LCV_scores %>%
    group_by(Year) %>%
    mutate('top25perc_val' = quantile(Nominal.Score, 0.75),
           'top50perc_val' = quantile(Nominal.Score, 0.5),
           'bottom25perc_val' = quantile(Nominal.Score, 0.25)) %>%
    mutate('top50perc_ind' = (Nominal.Score < top25perc_val & Nominal.Score >= top50perc_val),
           'bottom50perc_ind' = (Nominal.Score >= bottom25perc_val & Nominal.Score < top50perc_val)) %>%
    ungroup() %>%
    as.data.table()
  
  # Ed Markey - middle of 2013 rep to sen anomaly, appears twice. Remove his senate record. (14435)
  # Steven Kirk - Nov 2010 transition (20115)
  # Dean Heller - May 2011, keep senate now. (20730)
  LCV_scores <- LCV_scores[!(ICPSR == 14435 & Year == 2013 & Chamber == 2),]
  LCV_scores <- LCV_scores[!(ICPSR == 20115 & Year == 2010 & Chamber == 2),]
  LCV_scores <- LCV_scores[!(ICPSR == 20730 & Year == 2011 & Chamber == 1),]
  
  LCV_indicator_dset <- LCV_scores[,.(ICPSR, Year, Nominal.Score, Adj.Score, indicator_has_LCV_data = 1)]
  
  # Create new datasets that keep only the greenest candidates
  top50perc_cands <- LCV_scores[top50perc_ind == TRUE, .(ICPSR, Year, indicator_top_50perc = 1)]
  bottom50perc_cands <- LCV_scores[bottom50perc_ind == TRUE, .(ICPSR, Year, indicator_bottom_50perc = 1)]
  
  # # We want the scores from the year before.
  contribs <- contribs[, prev_year := (year(date) - 1)]
  
  # So now, the indicator is 1 only if in the previous year, the receiver had a LCV score (i.e. was in the LCV scores dataset).
  contribs <- merge.data.table(contribs, LCV_indicator_dset,
                               by.x = c('ICPSR2', 'prev_year'),
                               by.y = c('ICPSR', 'Year'),
                               all.x = TRUE)
  
  contribs <- merge.data.table(contribs, top50perc_cands,
                               by.x = c('ICPSR2', 'prev_year'),
                               by.y = c('ICPSR', 'Year'),
                               all.x = TRUE)
  contribs <- merge.data.table(contribs, bottom50perc_cands,
                               by.x = c('ICPSR2', 'prev_year'),
                               by.y = c('ICPSR', 'Year'),
                               all.x = TRUE)
  
  #### Make the final columns for analysis. We will only consider donations to LCV data candidates now
  #### for the quantiles but all donations for the green PACs.
  contribs <- contribs[, `:=`(TPD_cands = (TPD * indicator_cand * indicator_has_LCV_data),
                              TPD_PACs = (TPD * indicator_PAC),
                              ###### BIG & SMALL AMOUNTS
                              TPD_top_50perc = (TPD * indicator_cand * indicator_top_50perc),
                              TPD_bottom_50perc = (TPD * indicator_cand * indicator_bottom_50perc),
                              ###### BIG & SMALL COUNTS
                              Donation_count_top_50perc = (Donation_count * indicator_cand  * indicator_top_50perc),
                              Donation_count_bottom_50perc = (Donation_count * indicator_cand * indicator_bottom_50perc),
                              ###### SMALL AMOUNTS
                              TPD_top_50perc_below200 = (TPD_below200 * indicator_cand * indicator_top_50perc),
                              TPD_bottom_50perc_below200 = (TPD_below200 * indicator_cand * indicator_bottom_50perc),
                              ###### SMALL COUNTS
                              Donation_count_top_50perc_below200 = (Donation_count_below200 * indicator_cand * indicator_top_50perc),
                              Donation_count_bottom_50perc_below200 = (Donation_count_below200 * indicator_cand * indicator_bottom_50perc),
                              ###### BIG AMOUNTS
                              TPD_top_50perc_above200 = (TPD_above200 * indicator_cand * indicator_top_50perc),
                              TPD_bottom_50perc_above200 = (TPD_above200 * indicator_cand * indicator_bottom_50perc),
                              ###### BIG COUNTS
                              Donation_count_top_50perc_above200 = (Donation_count_above200 * indicator_cand * indicator_top_50perc),
                              Donation_count_bottom_50perc_above200 = (Donation_count_above200 * indicator_cand * indicator_bottom_50perc)
                              )]
  
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
                           'indicator_has_LCV_data',
                           'indicator_top_50perc',
                           'indicator_bottom_50perc') := NULL]
  
  # Since we are, for now, only considering donations to candidates, many NAs (when the donation was to a PAC),
  # So remove them.
  contribs <- contribs[!(is.na(TPD_cands) & (is.na(TPD_PACs) | TPD_PACs == 0)),]
  
  # Replace NAs with 0.
  f_dowle3 = function(DT) {
  for (j in names(DT)) {
    set(DT,which(is.na(DT[[j]])),j,0)
    }
  }
  
  # Now it will change them.
  f_dowle3(contribs)
  
  fwrite(contribs, glue('{cleaned}/zipquantiles2_contribs_{cycles[i]}.csv'))
  
  rm(contribs)
  
}

#### NEXT STEP : COMBINE THESE DATA INTO ONE.
list_of_datasets_by_year <- list.files(path = glue('{cleaned}/'), pattern = 'zipquantiles2_contribs_20*')

list_of_datasets_by_year

full_list <- list()
for(i in 1:length(list_of_datasets_by_year)) {

  contribs <- fread(glue('{cleaned}/{list_of_datasets_by_year[i]}'))
  contribs <- contribs[, Party := as.character(Party)]
  contribs <- contribs[, c('TPD_PACs',
                           'TPD_cands'
                           ) := NULL]
  full_list[[i]] <- contribs

}

full_df <- bind_rows(full_list)

# Write it as csv. Done.
fwrite(full_df, glue('{output}/zip_contribs_combined.csv'))

#########
#########
#########
#########
#########
# NEXT STEP, GO TO STATA, CONVERT DATES TO WEEKS, COME BACK HERE AND COLLAPSE TO COUNTY-WEEK LEVEL.

full_df <- fread(glue('{output}/zip_contribs_combined.csv'))

# Now collapse to county-week level, our final unit.
full_df <- full_df[, .(###### BIG & SMALL AMOUNTS
                       tpd_top_50perc = sum(tpd_top_50perc, na.rm = TRUE),
                       tpd_bottom_50perc = sum(tpd_bottom_50perc, na.rm = TRUE),
                       ###### BIG & SMALL COUNTS
                       donation_count_top_50perc = sum(donation_count_top_50perc, na.rm = TRUE),
                       donation_count_bottom_50perc = sum(donation_count_bottom_50perc, na.rm = TRUE),
                       ###### SMALL AMOUNTS
                       tpd_top_50perc_below200 = sum(tpd_top_50perc_below200, na.rm = TRUE),
                       tpd_bottom_50perc_below200 = sum(tpd_bottom_50perc_below200, na.rm = TRUE),
                       ###### SMALL COUNTS
                       donation_count_top_50perc_below2 = sum(donation_count_top_50perc_below2, na.rm = TRUE),
                       donation_count_bottom_50perc_bel = sum(donation_count_bottom_50perc_bel, na.rm = TRUE),
                       ###### BIG AMOUNTS
                       tpd_top_50perc_above200 = sum(tpd_top_50perc_above200, na.rm = TRUE),
                       tpd_bottom_50perc_above200 = sum(tpd_bottom_50perc_above200, na.rm = TRUE),
                       ###### BIG COUNTS
                       donation_count_top_50perc_above2 = sum(donation_count_top_50perc_above2, na.rm = TRUE),
                       donation_count_bottom_50perc_abo = sum(donation_count_bottom_50perc_abo, na.rm = TRUE),
                       fips = first(fips)),
                   by = .(contributorzipcode, week)]

# Check
fw <- full_df[, .(contributorzipcode, week)]
fw2 <- unique(fw)
# Yes.

fwrite(full_df, glue('{output}/zip_contribs_combined.csv'))

# NOW TAKE THIS BACK TO STATA, and get final output.
