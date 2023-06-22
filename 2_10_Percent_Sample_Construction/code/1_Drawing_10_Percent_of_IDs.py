"""
Amy Fan 1-2022

Drawing the 10% sample of consumer IDs

inputs: 
______

     ../../1_Full_Sample_Exploration/output/1_inandout.parquet

        file with when each person is in the dataset 
    
outputs:
______

    ../1_sample_ids.parquet

        Consumer IDs of the 10% 

"""

import pandas as pd
import numpy as np

output_1 = "..//..//1_Full_Sample_Exploration//output//"
output_2 = "..//output//"

##############
# READING IN #
##############

infilestr = output_1 + "1_inandout.parquet"

cols = [str(i) + "01" for i in range(2006, 2022)]

# just January's
jj = pd.read_parquet(infilestr, columns = cols)

# convert to a binary table 
jj = (jj == 1) * 1

################
# PANEL SAMPLE #
################

# create variable tracking how many January's someone is in the data
jj['total'] = jj.sum(axis = 1)

# only keep people who are in the data for more than one January 
jj = jj[jj.total != 0]

# track the first month someone is in the data
jj['first'] = 0 

# loop through each year and update
for year in range(2006, 2022): 
    
    colname = str(year) + "01"
    
    jj.loc[(jj[colname] == 1) & (jj['first'] == 0), 'first'] = year
    
# now, sort by this column
print(jj['first'].value_counts())
jj = jj.sort_values('first')

# pick every 10th person starting from the 8th person 
sample = jj.iloc[8::10, 0:1]
sample = sample.index

print(sample.shape)

# write the sample to a parquet 
outfilestr = output_2 + "1_sample_ids.parquet"

pd.DataFrame(sample).to_parquet(outfilestr)