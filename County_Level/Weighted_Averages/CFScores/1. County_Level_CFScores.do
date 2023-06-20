******
*	Author: Lorena Mita
*	Date:	2023-06-01

* Edited by Mohit Negi, 2023-06-06
* This code finds all the icpsr codes for each green PAC
******

clear all
global path "C:\Users\anjun\OneDrive\Desktop\EP\DIME\DIME_Data\Output\County_Level\Weighted_Averages\CFScores"
global path2 "C:\Users\anjun\OneDrive\Desktop\EP\DIME\DIME_Data\Raw"
global path3 "C:\Users\anjun\OneDrive\Desktop\EP\DIME\DIME_Data\Cleaned"

import delimited "$path\contribs_cf_combined.csv", varnames(1) clear

gen date2 = date(date, "YMD")
format date2 %td

drop date
rename date2 date

gen week = wofd(date)
*format week %tw

order week, after(date)

sort week fips

export delimited "$path\contribs_cf_combined.csv", replace






*** Come back to this after collapsing the above
*** in R.







clear all
global path "C:\Users\anjun\OneDrive\Desktop\EP\DIME\DIME_Data\Output\County_Level\Weighted_Averages\CFScores"
global path2 "C:\Users\anjun\OneDrive\Desktop\EP\DIME\DIME_Data\Raw"
global path3 "C:\Users\anjun\OneDrive\Desktop\EP\DIME\DIME_Data\Cleaned"

import delimited "$path\contribs_cf_combined.csv", varnames(1) clear

format week %tw

save "$path\contribs_cf_combined", replace

* Now FEMA data to create the regressors.
use "$path2\FEMA_disaster_county_clean", replace

* Remain in the relevant time frame
keep if start_datetime > date("01/01/2000", "MDY") & start_datetime < date("01/01/2015", "MDY")

ren (ymw fipscode) (week fips)
destring fips, replace

keep week fips
duplicates drop fips week, force					// keep only one event per county in a week

* This was here just to test, no need now.
* keep if fips == 35053

gen EWE = 1											
* dummy to signal one extreme weather event in a county in a week

expand 9, gen(new)
bysort fips week: gen n = _n

replace EWE = 0 if n != 5
bysort fips week: replace week = week + _n - 5

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

sort week fips
quietly by week fips:  gen dup = cond(_N==1,0,_n)

gen pre_dummy = max(pre_1w, pre_2w, pre_3w, pre_4w)
gen post_dummy = max(EWE, post_1w, post_2w, post_3w, post_4w)

bysort week fips: egen maxpre = max(pre_dummy)
bysort week fips: egen maxpost = max(post_dummy)
bysort week fips: gen both_pre_post = maxpre == maxpost

keep if both_pre_post == 1

sort week fips
by week fips: gen dup2 = cond(_N==1,0,_n)
drop if dup2 > 1
drop dup2 

expand 4, gen(new2)
bysort fips week: gen n2 = _n
bysort fips week: replace week = week + n2

sort week fips
by week fips: gen dup3 = cond(_N==1,0,_n)
drop if dup3 > 1
drop dup3 

keep week fips 
gen remove = 1
save "$path3\remove_these", replace

*************************************

use "$path2\FEMA_disaster_county_clean", replace

* Remain in the relevant time frame
keep if start_datetime > date("01/01/2000", "MDY") & start_datetime < date("01/01/2015", "MDY")

ren (ymw fipscode) (week fips)
destring fips, replace

keep week fips
duplicates drop fips week, force					// keep only one event per county in a week

* This was here just to test, no need now.
* keep if fips == 35053

gen EWE = 1											
* dummy to signal one extreme weather event in a county in a week

merge 1:1 week fips using "$path3\remove_these"
keep if _merge == 1

expand 9, gen(new)
bysort fips week: gen n = _n

replace EWE = 0 if n != 5
bysort fips week: replace week = week + _n - 5

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
sort week fips
merge 1:1 week fips using "$path\contribs_cf_combined"

*tsset fips week
*tsfill, full

keep if _merge != 2

save "$path\contribs_cf_combined_matched", replace













