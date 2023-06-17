#######################################
# MOHIT NEGI
# Last Updated : 19th May 2023
# Contact on : mohit.negi@studbocconi.it
# This script loads in the raw data, conducts matching to get ICPSR (id) numbers, and cleans it to
# make it compatible with the algorithm.
#######################################

##
library(tidyverse)
library(glue)
library(data.table)
library(stringdist)
library(fuzzyjoin)
##

house_scorecards_list <- list.files(path = './Data/Raw/', pattern = 'house-scorecard-grid-export.csv')
senate_scorecards_list <- list.files(path = './Data/Raw/', pattern = 'senate-scorecard-grid-export.csv')

house_scorecards_full_list <- list()
for(i in 1:length(house_scorecards_list)) {
  
  raw_data <- read.csv(glue('./Data/Raw/{house_scorecards_list[i]}'), skip = 6) 
  
  year <- str_sub(house_scorecards_list[i], 1, 4)
  
  clean_data <- raw_data %>% 
    select('District', 'Party', 'Member.of.Congress', glue('X{year}.Score')) %>% 
    mutate('Year' = year, 'Chamber' = 1) %>% 
    rename('Nominal.Score' = glue('X{year}.Score'), 'Name' = 'Member.of.Congress')
  
  house_scorecards_full_list[[i]] <- clean_data
  
}
house_scorecards_full_df <- do.call(rbind.data.frame, house_scorecards_full_list)
party_recoded = factor(house_scorecards_full_df$Party, levels = c('D', 'R'), labels = c(1, 0))
house_scorecards_full_df$Party <- party_recoded

senate_scorecards_full_list <- list()
for(i in 1:length(senate_scorecards_list)) {
  
  raw_data <- read.csv(glue('./Data/Raw/{senate_scorecards_list[i]}'), skip = 6) 
  
  year <- str_sub(senate_scorecards_list[i], 1, 4)
  
  clean_data <- raw_data %>% 
    select('State', 'Party', 'Member.of.Congress', glue('X{year}.Score')) %>% 
    mutate('Year' = year, 'Chamber' = 2) %>% 
    rename('Nominal.Score' = glue('X{year}.Score'), 'Name' = 'Member.of.Congress')
  
  senate_scorecards_full_list[[i]] <- clean_data
  
}
senate_scorecards_full_df <- do.call(rbind.data.frame, senate_scorecards_full_list)
party_recoded = factor(senate_scorecards_full_df$Party, levels = c('D', 'R'), labels = c(1, 0))
senate_scorecards_full_df$Party <- party_recoded

## Matching ICPSR id numbers.

congress_raw_old <- read.csv('./Data/Raw/legislators-historical.csv')
congress_raw_current <- read.csv('./Data/Raw/legislators-current.csv')

congress_raw <- rbind(congress_raw_old, congress_raw_current)

congress_ID_data <- congress_raw %>% 
  filter(party %in% c('Democrat', 'Republican', 'Independent')) %>% 
  select('last_name', 'first_name', 'state', 'district','party', 'icpsr_id') %>% 
  mutate('Name' = paste(last_name, first_name, sep = ', ')) %>% 
  select(-c('last_name', 'first_name')) %>% 
  unique()

party_recoded = factor(congress_ID_data$party, levels = c('Democrat', 'Republican'), labels = c(1, 0))
congress_ID_data$party <- party_recoded

house_ID_data <- congress_ID_data %>% 
  filter(!is.na(district))

house_ID_data$district <- str_pad(house_ID_data$district, width = 2, pad = '0')

house_matching_df <- house_ID_data %>% 
  mutate('District_full' = paste(state, district, sep = '-'))

senate_matching_df <- congress_ID_data %>% 
  filter(!is.na(state)) %>% 
  select(-'district')

colnames(house_scorecards_full_df) ; colnames(house_matching_df) ; colnames(senate_matching_df)

name_matcher <- function(string1, string2) {
  
  name_broken <- str_split(string1, pattern = ' ') %>% 
    str_remove_all(pattern = ',')
  
  name_broken2 <- str_split(string2, pattern = ' ') %>% 
    str_remove_all(pattern = ',')
  
  total_score = 0
  for(i in name_broken) {
    
    score <- stringdist(i, name_broken2) %>% 
      min()
    
    total_score <- total_score + score
    
  }
  
  match_quality <- total_score/length(name_broken)
  
  return(match_quality)
  
}

# First match based on party and district.
unique_names <- house_scorecards_full_df %>% 
    select('Name', 'Party', 'District') %>%
    unique()

prelim_match <- merge.data.frame(unique_names, house_matching_df,
                                 by.x = c('District', 'Party'),
                                 by.y = c('District_full', 'party'),
                                 all.x = TRUE)


prelim_match <- prelim_match %>% 
  rowwise() %>% 
  mutate('fuzzy_score' = name_matcher(Name.y, Name.x))

final_match_two_closest <- prelim_match %>% 
  group_by(Name.x) %>% 
  mutate('Rank' = rank(fuzzy_score)) %>% 
  filter(Rank < 3)

#write.csv(final_match_two_closest, './Data/Cleaned/house_two_closest_matches.csv')

# Do same for senators.

# First match based on party and state.
unique_names <- senate_scorecards_full_df %>% 
  select('Name', 'Party', 'State') %>%
  unique()

prelim_match <- merge.data.frame(unique_names, senate_matching_df,
                                 by.x = c('State', 'Party'),
                                 by.y = c('state', 'party'),
                                 all.x = TRUE)

prelim_match <- prelim_match %>% 
  rowwise() %>% 
  mutate('fuzzy_score' = name_matcher(Name.y, Name.x))

final_match_two_closest <- prelim_match %>% 
  group_by(Name.x) %>% 
  mutate('Rank' = rank(fuzzy_score)) %>% 
  filter(Rank < 3)

#write.csv(final_match_two_closest, './Data/Cleaned/senate_two_closest_matches.csv')

# Next, I weed out the bad matches by hand. This is done because many times the matching
# algo fails miserably, so cannot take any chances.

house_manual_matched <- read.csv('./Data/Cleaned/house_two_closest_matches_done.csv')
senate_manual_matched <- read.csv('./Data/Cleaned/senate_two_closest_matches_done.csv')

house_NAs <- house_manual_matched %>% 
  filter(is.na(Name.y)) %>% 
  rename('Name' = 'Name.x')
senate_NAs <- senate_manual_matched %>% 
  filter(is.na(Name.y)) %>% 
  rename('Name' = 'Name.x')

house_manual_matched <- house_manual_matched %>% 
  filter(Name.y != '')
senate_manual_matched <- senate_manual_matched %>% 
  filter(Name.y != '')

house_NAs <- house_NAs[!duplicated(house_NAs[,c('Name','District', 'Party')]),]
senate_NAs <- senate_NAs[!duplicated(senate_NAs[,c('Name','State', 'Party')]),]

final_match <- stringdist_left_join(house_NAs, senate_matching_df, by = 'Name')
#write.csv(final_match, './Data/Cleaned/house_NA_matches.csv')

final_match <- stringdist_left_join(senate_NAs, senate_matching_df, by = 'Name')
#write.csv(final_match, './Data/Cleaned/senate_NA_matches.csv')

house_manual_matched2 <- read.csv('./Data/Cleaned/house_NA_matches_done.csv')
senate_manual_matched2 <- read.csv('./Data/Cleaned/senate_NA_matches_done.csv')

# Now combine the manual matched ones.
house_manual_matched <- house_manual_matched %>% 
  select('District', 'Party', 'Name.x', 'icpsr_id') %>% 
  unique()
house_manual_matched2 <- house_manual_matched2 %>% 
  select('District', 'Party', 'Name.x', 'icpsr_id.y') %>%
  rename('icpsr_id' = 'icpsr_id.y') %>% 
  unique()

senate_manual_matched <- senate_manual_matched %>% 
  select('State', 'Party', 'Name.x', 'icpsr_id') %>% 
  unique()
senate_manual_matched2 <- senate_manual_matched2 %>% 
  select('State', 'Party', 'Name.x', 'icpsr_id.y') %>% 
  rename('icpsr_id' = 'icpsr_id.y') %>% 
  unique() 

house_final_df <- rbind(house_manual_matched, house_manual_matched2) %>% 
  rename('Name' = 'Name.x')
# John Dingell anomaly correction.
house_final_df[508,3] <- 2605

senate_final_df <- rbind(senate_manual_matched, senate_manual_matched2) %>% 
  rename('Name' = 'Name.x')

house_scorecards_unique <- house_scorecards_full_df[!duplicated(house_scorecards_full_df[,c('District', 'Party', 'Name')]),]
house_matched_semifinal <- left_join(house_scorecards_unique, house_final_df, by = c('Name'))
#write.csv(house_matched_semifinal, './Data/Cleaned/house_semifinal.csv')

senate_scorecards_unique <- senate_scorecards_full_df[!duplicated(senate_scorecards_full_df[,c('State', 'Party', 'Name')]),]
senate_matched_semifinal <- left_join(senate_scorecards_unique, senate_final_df, by = c('Name')) %>% 
  select('State.x', 'Name', 'icpsr_id', 'Party.x') %>% 
  rename('State' = 'State.x')


house_matched_semifinal <- read.csv('./Data/Cleaned/house_semifinal_done.csv')

# Keeping party now!
house_matched_semifinal <- house_matched_semifinal %>% 
  filter(Name != '') %>% 
  select('District.x', 'Name', 'icpsr_id', 'Party.x') %>% 
  rename('District' = 'District.x')

# Final match
house_matched_final <- left_join(house_scorecards_full_df, house_matched_semifinal,
                                 by = c('District', 'Name'))
nrow(house_matched_final) ; nrow(house_scorecards_full_df)

senate_matched_final <- left_join(senate_scorecards_full_df, senate_matched_semifinal,
                                 by = c('State', 'Name'))
nrow(senate_matched_final) ; nrow(senate_scorecards_full_df)

house_matched_final <- house_matched_final %>% 
  rename('ICPSR' = 'icpsr_id') %>% 
  select('ICPSR', 'Year', 'Chamber', 'Nominal.Score', 'Party.x')
senate_matched_final <- senate_matched_final %>% 
  rename('ICPSR' = 'icpsr_id') %>% 
  select('ICPSR', 'Year', 'Chamber', 'Nominal.Score', 'Party.x')

# Finally, combine the 2 and input it into the function
final_scorecard_list <- rbind(house_matched_final, senate_matched_final) %>% 
  filter(!is.na(ICPSR))

# Save it. Done.
write.csv(final_scorecard_list, './Data/Cleaned/cleaned_scorecards_withparty.csv')
