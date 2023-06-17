library(tidyverse)

# Int.Group.Deflate.R

# December 2009

# "zzz" indicates lines that need to be edited

# ADA

# The program takes a file of nominal interest group scores and computes a's and b's to convert
# them to "real" (or adjusted) scores.  See Groseclose, Levitt, and Snyder (APSR, 1999)
# for more details.

# Unlike the original (Matlab) code, this code uses a more efficient method to
# compute a's and b's.  Specifically it uses singular value decomposition.

# The data file that this program uses is listed in the first (non-comment) line
# of the "Load Data" section.  You'll need to edit this line. 

# The data must be in a text file with the following columns:
#       ICPSR Year Chamber Nominal.Score
# The first line of the data file must appear exactly as above.
# (Alternatively, you can set "header=F" in the read.table command.

# Chamber must be coded 1=House, 2=Senate

# Also, you'll need to edit the two "write.table" commands in the "write output files" section.  These
# lines specify the files to which the output is written.

# One of these files contains, for each legislator, the ICPSR number,
# Year, Chamber, Nominal Score, Real Score, and Average Real score.  The other contains the a's
# b's and x's that the program estimates.  Both files are written as text files.  If you want to 
# convert them to excel files, open excel, then - within excel - open the specific file.  (You will
# need to specify that the type of file is text.)  You should
# see the "Text Import Wizard".  Click on "Delimited" (as opposed to "Fixed Width").  Click "Next."
# Then click on "Space" in the "Delimiters" section.  Click "Next".  Then click "Finish."


########################## Clear memory ##############################

rm(list=ls())

################# Define constants and set parmeters #################

Base.Year = 2000
Base.Chamber = 1

# zzz
# Total.iter = 500   
tolerance  = 1e-6

# With ADA data, the following tolerance levels require the associated number of iterations:
# e-1: 19 iterations, e-2: 160, e-3: 650, e-4: 1670, e-5: 2730, e-6: 3790

########################## Load Data ################################

# zzz
raw.data = read.csv('./Data/Cleaned/cleaned_scorecards.csv')

# Remove missing values
raw.data <- raw.data %>% 
  filter(Nominal.Score != 'n/a')

# When you specif the folder name, make sure you use forward slashes, not back slashes

# sort rows by ICPSR number
raw.data = raw.data[order(raw.data[,'ICPSR']),]

ICPSR         = raw.data$ICPSR
Year          = raw.data$Year
Chamber       = raw.data$Chamber
Nominal.Score = raw.data$Nominal.Score

######################### Compute Key Parameters ######################

TotalObs = length(ICPSR)
Min.Year = min(Year)
Max.Year = max(Year)

TT = 2*(Max.Year - Min.Year + 1)
ones.TT  = matrix(1,TT,1)
zeros.TT = matrix(0,TT,1)

t0 = (Base.Chamber - 1)*(Max.Year - Min.Year + 1) + Base.Year - Min.Year + 1


# TT will be the number of columns in the matrix of Nominal Scores.  It equals the number of years times 2.
# It is multiplied by 2 because there are 2 chambers.  (The program assumes that it is given House AND Senate
# data


######################### Count Unique ICPSR numbers ######################

N = 0   # N equals the total number of legislators - ie number of unique ICPSR's
Current.ICPSR = 0  # Assumes that no legislator is given 0 as an ICPSR number

for (j in 1:TotalObs) {
  if (Current.ICPSR < ICPSR[j]) {
    N = N + 1
    Current.ICPSR = ICPSR[j]
  }
}

zeros.N = matrix(0,N,1)
ones.N  = matrix(1,N,1)

################ Create an N x TT matrix of Nominal Scores #################

# Nominal.Score.Matrix will be an N x TT matrix.  The first row will be the scores 
# for legislator with the lowest ICPSR number.  In that row, the first entry
# will be the score that he received in the Min.Year, while serving in the
# House.  If he did not serve in Min. Year, or if he served in the Senate
# that year, this entry will be NA.  The second entry will be the score that
# he received in Min.Year+1, while serving in the House, and so on.
# The second half of the entries in the row will be the legislator's scores
# while serving in the Senate.  Since a legislator can only serve in one
# chamber at a time, at least half the entries in any row will be NA.

# Set up the Nominal.Score.Matrix as a bunch of NA's
Nominal.Score.Matrix = matrix(NA,N,TT)

n = 0
current.ICPSR = 0  # Assumes no legislator has 0 for an ICPSR

for (j in 1:TotalObs) {
  
  if (current.ICPSR != ICPSR[j]) { 
    n=n+1
    current.ICPSR = ICPSR[j] 
  }
  tt = (Chamber[j]-1)*(Max.Year-Min.Year+1) + (1+Year[j]-Min.Year)
  Nominal.Score.Matrix[n,tt] = Nominal.Score[j]
  
}

dim(Nominal.Score.Matrix)

#Nominal.Score.Matrix <- mapply(Nominal.Score.Matrix, FUN = as.numeric)

################### Create seed values for a, b, and x #####################

a = zeros.TT
b = ones.TT
x = zeros.N
x.prime = zeros.N   
b.prime = ones.TT
# x' and b' come from the singular value decomposition.  I just normalize the length of the 
# vector here so it will be TT x 1, instead of 1 x TT

### seed x = average nominal score

NA.ind = is.na(Nominal.Score.Matrix)

for (i in 1:N) {
  
  score.sum = 0
  score.count = 0
  
  for (j in 1:TT) {
    if (NA.ind[i,j] == F) {
      score.sum = score.sum + as.numeric(Nominal.Score.Matrix[i,j])
      score.count = score.count + 1
    }
  }
  
  x[i] = score.sum/score.count
  
}

head(x)

set.seed(92734)

#     ########## Set seed values for a's and b's ############
a = sample(c( 13.255,  14.432,   2.816,   1.359,   2.015,   1.191,  12.877,  12.219,  16.992,  26.322,  21.868,   7.307,
      5.568,   5.360,  -4.896,   4.270,  -0.657,  -0.657, -11.543,  12.851,  -5.072,  -7.244,  -7.025,  -0.612,
     -3.465,  -9.012,  -2.787,   0.211,  -4.924,  -5.967,  -4.257,   0.770,   0.689,   0.000,  -2.123,   0.606,
      1.799,   1.227,   2.312,   0.380,   3.499,   7.807,   0.441,   3.156,  -1.045,   7.207,   0.636,  -1.126,
     -4.660,  -1.540,   1.273,  -0.649,   5.827,  24.460,  26.007,   9.678,  17.338,  11.591,  15.767,  15.697,
     14.031,   2.410,  18.959,  25.601,  13.211,   0.091,   4.168,   4.135,  -0.353,   4.225,   4.220,  -0.974,
     -4.047,  -0.668,  -0.070,   1.413,   0.451,   0.551,  -1.848,   1.731,   2.267,   0.911,  -0.491,  -0.031,
      5.501,   7.153,  12.309,  -1.214,   5.212,   5.898,   4.845,  -3.143,   0.530,   7.649,   0.717,  -1.443,
      2.216,   1.419,   5.904,   7.064,   4.007,  -5.322,  -0.442,   2.487,  -2.108,  -5.148), 46)

b = sample(c(1.1225, 0.9894, 1.2440, 1.1711, 1.1557, 1.2160, 1.0375, 1.0462, 0.9828, 0.9827, 0.8897, 1.1413, 1.2017,
    1.2858, 1.3958, 1.2412, 1.2390, 1.2389, 1.2225, 1.2314, 1.0805, 1.1407, 1.0425, 0.9835, 1.0097, 1.0299,
    1.0226, 0.9016, 1.0774, 0.9645, 0.9218, 0.7693, 0.9410, 1.0000, 0.9615, 0.9880, 1.0327, 0.9922, 0.9556,
    1.0139, 1.0341, 0.9831, 1.0520, 0.9854, 0.9901, 0.9713, 1.0238, 0.9940, 1.0760, 1.0039, 0.9990, 1.0891,
    1.0287, 1.1355, 1.0621, 1.3254, 1.0484, 1.1968, 1.1805, 0.9994, 1.0736, 1.0975, 1.1345, 0.8322, 1.1244,
    1.2483, 1.2822, 1.3419, 1.1973, 1.1634, 1.1635, 1.2232, 1.2201, 1.0309, 0.9353, 1.1882, 1.0779, 1.1356,
    1.0018, 1.0535, 1.0596, 1.0668, 0.9649, 0.9999, 0.8020, 0.7827, 0.8982, 1.0845, 1.0700, 1.0043, 1.1212,
    1.1238, 1.1167, 1.0630, 1.1217, 1.0703, 1.0692, 1.1058, 1.1191, 1.0224, 1.1089, 1.2952, 1.1687, 1.0767,
    1.2161, 1.3247), 46)
#
####### Create seed values for x's
### This overwirtes the above method, which computes x as a simple
### average of Nominal Scores
#
#for (n in 1:N) {
#  Num.sum = 0
#  Denom.sum = 0
#
#  for (tt in 1:TT) {
#    if (NA.ind[n,tt] == F) {
#      Num.sum = Num.sum + b[tt]*(Nominal.Score.Matrix[n,tt]-a[tt])
#      Denom.sum = Denom.sum + (b[tt])^2
#    }
#  }
#  x[n] = Num.sum/Denom.sum
#}
#

#################### Compute a's, b's, and x's ############################

# Set Median Change in x to a super-high initial value
Mean.Change.x = 999

iter = 0 

while (Mean.Change.x > tolerance) {
  #for (iter in 1:Total.iter) {
  
  iter = iter + 1  # Delete this if you use the for loop instead of the while loop 
  print(iter)
  
  a.old = a
  b.old = b
  x.old = x
  
  ## fill in missing values of Nom Score matrix with expected
  ## values, ie a_t + b_t x_i
  
  #Nominal.Score.Matrix.Filled.In = Nominal.Score.Matrix
  #for (i in 1:N) {
  #  for (j in 1:TT) {
  #    if (NA.ind[i,j] == T) { Nominal.Score.Matrix.Filled.In[i,j] = a[j]+b[j]*x[i] }
  #  }
  #}
  
  Predicted.Values = ones.N %*% t(a) + x %*% t(b)
  Nominal.Score.Matrix.Filled.In = ifelse(NA.ind == T, Predicted.Values, Nominal.Score.Matrix)
  
  ## Compute mean for each year
  y.bar = apply(Nominal.Score.Matrix.Filled.In, 2, FUN = function(x) mean(as.numeric(x)))
  
  ## Create Z matrix, the Nominal-Score Matrix (filled in) with each year's
  ## mean subtracted from each entry.
  ## Thus, each year's (i.e. column's) mean score of Z will be zero.
  Z = as.numeric(Nominal.Score.Matrix.Filled.In) - ones.N %*% t(y.bar)
  
  ## Use singular value decomposition to compute a_t' , b_t' and x_i'.  These
  ## are parameters that best predict z_it.  I.e.
  ## z_it^hat = a_t' + b_t' x_i'
  
  sing = La.svd(Z,nu=1,nv=1)   
  # returns, u, d, and v, such that Z = uDv, where D is a diagonal
  # matrix with diagonal elements equal to the vector d.
  
  x.prime = sing$u*N  
  # Later we'll normalize x' and b' -e.g. I'll divide each b_t' by b_t0', where t0 is the base year.
  # I multiply by N so the average x' is about 50 (instead of .5).  
  # All that is important is x'*b'.  Later, I'll divide b' by N to cancel it out, and also to make the 
  # average b' about 1.0 (instead of 10 or .1).  
  
  b.prime = t(sing$d[1]*sing$v/N)
  
  # define base t [ie the base period]
  
  #### Compute a, b, and x.  That is, find parameters to predict not Z (mean-adjusted Nom Scores),
  #### but the actual nom scores.  To do this we have to convert x' and b' to xb so that x'b' equals
  #### xb, while b_t0 is set to 1.0.  Also, we need to find a's such that y = y-bar + z, and y = a+bx,
  #### while a_t0 is zero.  It takes about two pages of algebra, but you can show that if you set a's
  #### b's, and x's as below, you'll satisfy all of the above constraints.
  a = y.bar - (y.bar[t0]/b.prime[t0])*b.prime
  b = b.prime / b.prime[t0]     
  x = x.prime*b.prime[t0] + y.bar[t0]*ones.N
  
  x.abs.diff = abs(x.old - x)
  Mean.Change.x = mean(x.abs.diff) 
  
}


## Compute Mean Squared Error
Mean.Sq.Error = 0
for (n in 1:N) {
  for (tt in 1:TT) {
    Mean.Sq.Error=Mean.Sq.Error+(as.numeric(Nominal.Score.Matrix.Filled.In[n,tt])-a[tt]-b[tt]*x[n])^2
  }
}
Mean.Sq.Error = Mean.Sq.Error/(TotalObs)
print(Mean.Sq.Error)

############################ Generate Adjusted Scores ###################################

# Create matrix that is one column wide to store the scores
Adj.Score = matrix(NA, nrow=TotalObs, ncol=1)	

for (q in 1:TotalObs) {       
  
  # Recall that the vector of a's is listed as: 1947 House a, 1948 House a, 
  # 1949 House a, ... 2008 House a, 1947 Senate a, 1948 Senate ... 2008 Senate
  # The vector of b's is listed in the same manner
  ab.index =  (Year[q] - Min.Year + 1) + (Chamber[q] - 1)*(Max.Year - Min.Year + 1)
  
  Adj.Score[q] = (as.numeric(Nominal.Score[q]) - a[ab.index]) / b[ab.index]	
}

################## attach adjusted scores to the original dataset  ########################

# Create Final.Data matrix - equals Adj.Scores attached to the right of raw.data matrix
Final.Data = cbind(raw.data, Adj.Score)

########################## Calculate mean adjusted scores #################################

# Create a matrix of NA's
Mean.Adj.Score = matrix(nrow=nrow(Final.Data), ncol=1, NA)

# Attach Mean.Adj.Scores to the right of the Final.Data matrix
Final.Data = cbind(Final.Data, Mean.Adj.Score)

Unique.ICPSR.Vector = unique(Final.Data$ICPSR)
Unique.ICPSR.Vector <- Unique.ICPSR.Vector[!(is.na(Unique.ICPSR.Vector))]

for (Current.ICPSR in Unique.ICPSR.Vector)  {
  
  # Create the vector of Adj.Scores for the legislator with ICPSR number equal to Current.ICPSR
  Current.Scores = Final.Data[Final.Data$ICPSR==Current.ICPSR, "Adj.Score"]
  
  # For each row containing data for legislator j, enter the mean of his adjusted scores
  # in the column "Mean.Adj.Scores"
  Final.Data[Final.Data$ICPSR==Current.ICPSR & !(is.na(Final.Data$ICPSR)), "Mean.Adj.Score"] = mean(Current.Scores, na.rm=T)
}

################################### Write output files #########################################

write.csv(Final.Data, './Data/Output/harmonized_scorecards.csv')


######### Write a file with a's, b's, x's and MSE
Params = matrix(nrow=nrow(x), ncol=4, NA)
Params[1:length(a), 1] = a
Params[1:length(b), 2] = b
Params[1:length(x), 3] = x
Params[1:length(Mean.Sq.Error), 4] = Mean.Sq.Error
colnames(Params) = c("a", "b", "x", "MSE")

write.csv(Params, './Data/Output/parameters_test.csv')
