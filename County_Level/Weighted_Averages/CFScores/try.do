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

use "$path\contribs_cf_combined", clear

keep if fips == 31153










