################## 
# MOHIT NEGI
# Last Updated : 16th June 2023
# Contact on : mohit.negi@studbocconi.it
################## LIBRARIES
library(glue)
library(googledrive)
################## 

################## PATHS
harddrive <- 'D:/Mohit_Work/DIME/ContribDB'
################## 

# Note down the cycles we want
cycles <- c(1998, 2000, 2002, 2004, 2006, 2008, 2010, 2012, 2014)

# Collect the google drive links for all cycles. 1998, 2000,2,4,6,8,10,12 and 14.
gdrive_links <- c(
  'https://drive.google.com/file/d/0B5SFXStyeVhrT1Q2QkNUS2o1a2s/view?usp=drive_link&resourcekey=0-W59JmWAvDu1YU_4UgRAmaA',
  'https://drive.google.com/file/d/0B5SFXStyeVhrVmdxZEhPaW0ydkE/view?usp=drive_link&resourcekey=0-SEscaAe1dLhLQQZdMRvpCA',
  'https://drive.google.com/file/d/0B5SFXStyeVhreFRSME83RHNLbzg/view?usp=drive_link&resourcekey=0-WK3hDxfWpKwul8jZ9CGadA',
  'https://drive.google.com/file/d/0B5SFXStyeVhreDZaal90VkkxRjA/view?usp=drive_link&resourcekey=0-8SzOaEMsfqjH5Y3FDpFuaA',
  'https://drive.google.com/file/d/0B5SFXStyeVhrNGpvMlN2RTF4Mm8/view?usp=drive_link&resourcekey=0-GmtyfpX1KzrrzNNipDwXeQ',
  'https://drive.google.com/file/d/0B5SFXStyeVhrRGVxaXl0RE4yS0U/view?usp=drive_link&resourcekey=0-TCheXnAvjvXcp9oqTHJqgg',
  'https://drive.google.com/file/d/0B5SFXStyeVhrdldhSVdtUWQ2NHM/view?usp=drive_link&resourcekey=0-CAqOZe9UKLYgZfW5ccd7yg',
  'https://drive.google.com/file/d/0B5SFXStyeVhrUk9pM21VV2ZyN3c/view?usp=drive_link&resourcekey=0-cs2Fe6EjKbniZ8_EZTGYkg',
  'https://drive.google.com/file/d/0B5SFXStyeVhrb1JnaVYzT2p6WWM/view?usp=drive_link&resourcekey=0-WEXQFkUpA46AEUev_ucR7w'
)

for(i in 1:length(cycles)) {
  
  print(glue('Loop is on : {cycles[i]}'))
  
  # Download the data from Google Drive of Adam Bonica (DIME)
  drive_download(
    gdrive_links[i],
    path = glue('{harddrive}/contribDB_{cycles[i]}.csv'),
    type = NULL,
    overwrite = TRUE
  )
  
}
