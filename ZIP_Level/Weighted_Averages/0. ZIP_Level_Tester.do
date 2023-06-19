******
*	Author: Lorena Mita
*	Date:	2023-06-01

* Edited by Mohit Negi, 2023-06-06
* This code finds all the icpsr codes for each green PAC
******

clear all
global path "C:\Users\anjun\OneDrive\Desktop\EP\DIME\DIME_Data\Output\ZIP_Level\Weighted_Averages"
global path2 "C:\Users\anjun\OneDrive\Desktop\EP\DIME\DIME_Data\Raw"
global path3 "C:\Users\anjun\OneDrive\Desktop\EP\DIME\DIME_Data\Cleaned"

import delimited "$path\test_zip_contribs_combined.csv", varnames(1) clear

gen date2 = date(date, "YMD")
format date2 %td

drop date
rename date2 date

gen week = wofd(date)
*format week %tw

order week, after(date)

sort week contributorzipcode

export delimited "$path\test_zip_contribs_combined.csv", replace






*** Come back to this after collapsing the above
*** in R.







clear all
global path "C:\Users\anjun\OneDrive\Desktop\EP\DIME\DIME_Data\Output\ZIP_Level\Weighted_Averages"
global path2 "C:\Users\anjun\OneDrive\Desktop\EP\DIME\DIME_Data\Raw"
global path3 "C:\Users\anjun\OneDrive\Desktop\EP\DIME\DIME_Data\Cleaned"

import delimited "$path\test_zip_contribs_combined.csv", varnames(1) clear

format week %tw

save "$path\test_zip_contribs_combined", replace

****
use "$path\test_zip_contribs_combined", replace

keep contributorzipcode fips
duplicates drop

save "$path3\zip_to_fips", replace
****

* Now FEMA data to create the regressors.
use "$path2\FEMA_disaster_county_clean_zips", replace

drop fips
* Remain in the relevant time frame
keep if start_datetime > date("01/01/2000", "MDY") & start_datetime < date("01/01/2015", "MDY")

ren (ymw fipscode) (week fips)
destring fips, replace

keep week contributorzipcode
duplicates drop week contributorzipcode, force				
* keep only one event per zip in a week

gen EWE = 1											
* dummy to signal one extreme weather event in a county in a week

expand 9, gen(new)
bysort week contributorzipcode: gen n = _n

replace EWE = 0 if n != 5
bysort week contributorzipcode: replace week = week + _n - 5

gen pre_1w = 0
replace pre_1w = 1 if EWE == 0 & n == 4
gen pre_2w = 0
replace pre_2w = 1 if EWE == 0 & n == 3
gen pre_3w = 0
replace pre_3w = 1 if EWE == 0 & n == 2
gen pre_4w = 0
replace pre_4w = 1 if EWE == 0 & n == 1

gen post_1w = 0
replace post_1w = 1 if EWE == 0 & n == 6
gen post_2w = 0
replace post_2w = 1 if EWE == 0 & n == 7
gen post_3w = 0
replace post_3w = 1 if EWE == 0 & n == 8
gen post_4w = 0
replace post_4w = 1 if EWE == 0 & n == 9

sort week contributorzipcode
quietly by week contributorzipcode:  gen dup = cond(_N==1,0,_n)

gen pre_dummy = max(pre_1w, pre_2w, pre_3w, pre_4w)
gen post_dummy = max(EWE, post_1w, post_2w, post_3w, post_4w)

bysort week contributorzipcode: egen maxpre = max(pre_dummy)
bysort week contributorzipcode: egen maxpost = max(post_dummy)
bysort week contributorzipcode: gen both_pre_post = maxpre == maxpost

keep if both_pre_post == 1

sort week contributorzipcode
by week contributorzipcode: gen dup2 = cond(_N==1,0,_n)
drop if dup2 > 1
drop dup2 

expand 4, gen(new2)
bysort week contributorzipcode: gen n2 = _n
bysort week contributorzipcode: replace week = week + n2

sort week contributorzipcode
by week contributorzipcode: gen dup3 = cond(_N==1,0,_n)
drop if dup3 > 1
drop dup3 

keep week contributorzipcode 
gen remove = 1
save "$path3\remove_these_zips", replace

*************************************

use "$path2\FEMA_disaster_county_clean_zips", replace

drop fips
* Remain in the relevant time frame
keep if start_datetime > date("01/01/2000", "MDY") & start_datetime < date("01/01/2015", "MDY")

ren (ymw fipscode) (week fips)
destring fips, replace

keep week contributorzipcode
duplicates drop week contributorzipcode, force				
* keep only one event per county in a week

gen EWE = 1											
* dummy to signal one extreme weather event in a county in a week

merge 1:1 week contributorzipcode using "$path3\remove_these_zips"
keep if _merge == 1

expand 9, gen(new)
bysort week contributorzipcode: gen n = _n

replace EWE = 0 if n != 5
bysort week contributorzipcode: replace week = week + _n - 5

gen pre_1w = 0
replace pre_1w = 1 if EWE == 0 & n == 4
gen pre_2w = 0
replace pre_2w = 1 if EWE == 0 & n == 3
gen pre_3w = 0
replace pre_3w = 1 if EWE == 0 & n == 2
gen pre_4w = 0
replace pre_4w = 1 if EWE == 0 & n == 1

gen post_1w = 0
replace post_1w = 1 if EWE == 0 & n == 6
gen post_2w = 0
replace post_2w = 1 if EWE == 0 & n == 7
gen post_3w = 0
replace post_3w = 1 if EWE == 0 & n == 8
gen post_4w = 0
replace post_4w = 1 if EWE == 0 & n == 9

drop remove n new _merge
*tsset fips week
*tsfill, full

* Final Merge.
sort week contributorzipcode
merge 1:1 week contributorzipcode using "$path\test_zip_contribs_combined"

*tsset fips week
*tsfill, full

keep if _merge != 2

save "$path\test_zip_contribs_combined_matched", replace













