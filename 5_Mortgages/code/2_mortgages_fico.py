"""

Amy Fan 2-2022

Seeing what percentage of people have at least one mortgage each month, broken out by FICO score

    inputs: 
    ______
    
        ~/projects/equifaxmacro_proj/EquiFax3/2_10_Percent_Sample_Construction/output/sample/{YEARMON}_10perc.parquet
    
            10 percent sample of the full dataset 
    
    outputs: 
    _______

        ../output/2_mort_fico.csv
    
            File with zip codes of all mortgages 
        
"""

import pandas as pd
import numpy as np

sample = "..//..//2_10_Percent_Sample_Construction//output//sample//"
output_5 = "..//output//"
output_3 = "..//..//3_Balance_In_And_Out_Construction//output//"

#############
# FUNCTIONS #
#############

# convert number to DATE string
def monyr(monnum):

    mon = (monnum + 5) % 12 + 1

    if mon < 10: 
        MONNUM = "0" + str(mon)
    else: 
        MONNUM = str(mon)

    YRNUM = int(((monnum - mon)/12) + 2006)

    DATE = str(YRNUM) + MONNUM
    
    return DATE;

##############################################
# GET THE FIRST MONTH SOMEONE HAS A MORTGAGE #
##############################################

# we only want one column (plus the index) 
biao_cols = ['first_c']
biao_str = output_3 + "1_balance_in_and_out.parquet"

# read in the file
biao = pd.read_parquet(biao_str, columns = biao_cols).reset_index()

# only keep mortgage tradelines and drop irrelevant index variables
biao_mort = biao[biao.product_category == "FM"].drop(['trade_id', 'product_category'], axis = 1)

# only keep the start date of the first mortgage someone has 
biao_mort = biao_mort.sort_values('first_c').drop_duplicates('consumer_id', keep = 'first')

######################### 
# LOOPING THROUGH FILES #
#########################

end = 195 # 195

results = []

# relevant columns 

variables = ['consumer_id', 'product_category', 'vantage_score_3']

# looping through all the months

for month in range (1, end + 1): 
    
    infile = sample + monyr(month) + "_10perc.parquet"
    df = pd.read_parquet(infile, columns = variables)
    
    df['vant_cat'] = 0
    df.loc[df['vantage_score_3'] < 700, 'vant_cat'] = 1
    df.loc[(df['vantage_score_3'] < 750) & (df['vantage_score_3'] >= 700), 'vant_cat'] = 2
    df.loc[df['vantage_score_3'] >= 750, 'vant_cat'] = 3

    # the number of consumer_ids that month broken out by fico score
    cids_fico = df[['consumer_id','vant_cat']].drop_duplicates().vant_cat.value_counts()

    mort_count = df[df['product_category'] == "FM"]
    mort_count = mort_count[['consumer_id', 'vant_cat']].value_counts().reset_index()
    cids_mort = mort_count['vant_cat'].value_counts().sort_index()

    row1 = cids_mort/cids_fico
    
    # figure out the percentage of people who have ever gotten a mortgage by vantage score 
    
    # we want to look at people who have ever had a mortgage before that month
    biao_mort_temp = biao_mort[biao_mort.first_c <= month].consumer_id
    
    # keep the vantage score of people 
    mort_ever = df.loc[df.consumer_id.isin(biao_mort_temp),
                        ['consumer_id', 'vant_cat']].drop_duplicates().vant_cat.value_counts().sort_index()
    
    row2 = mort_ever/cids_fico
    
    thisrow = pd.concat([row1, row2])
    thisrow.index = ["Vantage<700", "700<Vantage<750", "750<Vantage", "Vantage<700_ever", "700<Vantage<750_ever", "750<Vantage_ever"]
    
    results.append(thisrow)
    print(f"done with month {month}, {monyr(month)}", flush = True)

print(results)

final = pd.concat(results, axis = 1, ignore_index = True)
final = final.transpose()

outfile = output_5 + "2_mort_fico.csv"
final.to_csv(outfile) 