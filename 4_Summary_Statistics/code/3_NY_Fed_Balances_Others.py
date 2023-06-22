"""
Amy Fan 2-2022

Replicating various NY Fed graphs from this report: https://www.newyorkfed.org/medialibrary/interactives/householdcredit/data/pdf/HHDC_2021Q3.pdf

Drops half of all joint tradelines 

inputs: 
______

    ../../2_10_Percent_Sample_Construction/output/sample/{YEARMON}_10perc.parquet
    
        10 percent sample of the full dataset 


outputs: 
_______

    ../output/3_other_stats.csv
    
        various summary statistics on debt balance by quarter

    ../output/3_debt_by_type_per_capita.csv
    
        debt by type per capita for the last quarter
    
    ../output/3_debt_by_age_by_type.csv

        debt by age per capita for the last quarter

        
"""
##############
# SETTING UP #
##############

import pandas as pd
import numpy as np

sample = "..//..//2_10_Percent_Sample_Construction//output//sample//"
output_4 = "..//output//"

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


# function to recode the equifax product categories
def recode_nyfrb(non_other): 
    
    # mortgage 
    non_other.loc[non_other['product_category'].isin(['FM', 'HI']), 'product_category'] = 'first_mortgage'

    # home equity revolving 
    non_other.loc[non_other['product_category'] == 'HR', 'product_category'] = 'he_revolving'

    # auto loan 
    non_other.loc[non_other['product_category'].isin(['AB2', 'AF2']), 'product_category'] = 'auto'

    # credit card
    non_other.loc[non_other['product_category'].isin(['BC', 'RT']), 'product_category'] = 'credit_card'

    # student loan
    non_other.loc[non_other['product_category'].isin(['SL1', 'SL2']), 'product_category'] = 'student_loan'
    
    # other
    non_other.loc[~non_other.product_category.isin(['first_mortgage', 'he_revolving', 'auto', 'credit_card', 'student_loan']), 'product_category'] = 'other'
    
    return non_other;

####################################
# READING IN QUARTERLY INFORMATION #
####################################

# we want to make one giant row for each quarter. Let's put the values in a list for now
allrowlist = []

# For page 17, we need to track the number of *new* foreclosures and bankruptcies. Let's keep a running list here
foreclosure_ids = pd.Series(dtype='float64')
bankruptcies_ids = pd.Series(dtype='float64')

# in retrospect, these are all the columns we want
moncols = ['consumer_id', 'inquiries_12_months', 'number_accounts_opened_within_12_months'] + \
            ['new_origination_flag', 'origination_vantage_score3', 'product_category'] + \
            ['status_category', 'balance'] + \
            ['number_3rd_party_collection_accts','total_amount_3rd_party_collections'] + \
            ['bankruptcy_flag', 'foreclosure_flag'] + \
            ['state'] + \
            ['consumer_age_archive'] + \
            ['ecoa'] 
            
# now let's just read in one quarter at a time

end = 65 # 65 quarters total 

for quarternum in range(1, end + 1): 
    
    monnum = 3 * quarternum 
    DATE = monyr(monnum)
    
    # read in the month for that quarter
    mon1_str = sample + DATE + "_10perc.parquet"
    mon1 = pd.read_parquet(mon1_str, columns = moncols)

    # let's convert all the product_categories
    mon1 = recode_nyfrb(mon1)
    
    # half all the balances that are joint tradelines 
    mon1.loc[mon1.ecoa == "J", 'balance'] = mon1.loc[mon1.ecoa == "J", 'balance'] * 0.5
    
    # now, as we go through each graph, we want to store the results in a list to append to a big row at the end
    rowlist = []
    
    ##################################################
    # PAGE 3: TOTAL DEBT BALANCE AND ITS COMPOSITION #
    ##################################################
    
    total_debt = mon1[['product_category', 'balance']].groupby('product_category').sum()
    total_debt.index = total_debt.index + "_total"
    rowlist.append(total_debt)
    
    ######################################################
    # PAGE 5: TOTAL NUMBER OF NEW ACCOUNTS AND INQUIRIES #
    ######################################################

    # clean the exception values to zero 
    mon1.loc[mon1['inquiries_12_months']>=92, 'inquiries_12_months'] = 0
    mon1.loc[mon1['number_accounts_opened_within_12_months']>=92, 'number_accounts_opened_within_12_months'] = 0

    # generate the statistics 
    acc_iq = mon1[['consumer_id', 'inquiries_12_months', 'number_accounts_opened_within_12_months']].drop_duplicates()
    acc_iq_sum = acc_iq[['inquiries_12_months', 'number_accounts_opened_within_12_months']].sum()

    # add to the list 
    rowlist.append(acc_iq_sum)

    
    ##############################################################
    # PAGES 7+9: CREDIT SCORE AT ORIGINATION: MORTGAGES AND AUTO #
    ##############################################################

    # subset to the variables we want
    orig_cred = mon1[['new_origination_flag', 'origination_vantage_score3', 'product_category']]

    # find people with new mortgages and calculate percentiles 
    mort = orig_cred[(orig_cred['product_category'] == "first_mortgage") & (orig_cred['new_origination_flag'] == 1)]
    mp = mort['origination_vantage_score3'].describe(percentiles = [0.1, 0.25, 0.5]).iloc[4:7]

    # find people with new auto loans and calculate percentiles 
    auto = orig_cred[(orig_cred['product_category'] == "auto") & (orig_cred['new_origination_flag'] == 1)]
    ap = auto['origination_vantage_score3'].describe(percentiles = [0.1, 0.25, 0.5]).iloc[4:7]

    # create the values for the rows and make labels
    row = pd.DataFrame(pd.concat([mp, ap]))
    row.index = ['mort_10%', 'mort_25%', 'mort_50%', 'auto_10%', 'auto_25%', 'auto_50%']

    # rename the index and append
    row.index = row.index + "_orig_vant3"
    rowlist.append(row)
    
    
    ################################################
    # PAGE 11: TOTAL BALANCE BY DELINQUENCY STATUS #
    ################################################
    
    # subset to relevant variables 
    derog_vars = ['status_category', 'balance']
    derog = mon1[derog_vars]

    # group by status_category and add in codebook info
    derog_stat = derog.groupby('status_category').sum()/derog.balance.sum()
    derog_stat.index = ['Miscellaneous',
                        'Current',
                        '30 DPD',
                        '60 DPD',
                        '90 DPD',
                        '120 DPD or Collections',
                        'Foreclosure Started',
                        'Closed-Positive',
                        'Closed-Severe Derogatory',
                        'Closed-Bankruptcy']

    # rename index and add 
    derog_stat.index = derog_stat.index + "_perc"
    rowlist.append(derog_stat)
    
    ################################################################
    # PAGE 12: PERCENT OF BALANCE 90+ DAYS DELINQUENT BY LOAN TYPE #
    ################################################################
    
    # subset to relevant variables 
    derog_type_vars = ['product_category', 'status_category', 'balance']
    derog_type = mon1[derog_type_vars]
    
    # flag all the balances that are more than 90+ days delinquent (status_category >= 4)
    derog_type['sd'] = (derog_type.status_category >= 4) * 1
    
    # figure out what percentage of balances are 90+ days delinquent 
    derog_type_row = derog_type.loc[derog_type.sd==1, ['product_category', 'balance']].groupby(['product_category']).sum()/derog_type[['product_category', 'balance']].groupby('product_category').sum()
    
    # rename index and append 
    derog_type_row.index = derog_type_row.index + "_derog_perc"
    rowlist.append(derog_type_row)

    
    ####################################################
    # PAGE 17: NUMBER OF FORECLOSURES AND BANKRUPTCIES #
    ####################################################
    
    # subset to relevant variables (dropping duplicates bc this is consumer level information)    
    fb_variables = ['consumer_id','bankruptcy_flag', 'foreclosure_flag']
    fb = mon1[fb_variables].drop_duplicates()
   
    # clean exception code variables 
    fb.loc[fb['bankruptcy_flag']>=7, 'bankruptcy_flag'] = 0

    # clean exception code variables 
    fb.loc[fb['foreclosure_flag']>=7, 'foreclosure_flag'] = 0
    
    # keep the new bankruptcy people
    nb = fb.loc[~fb.consumer_id.isin(bankruptcies_ids) & (fb.bankruptcy_flag==1)]
    
    # keep the new foreclosure people
    nf = fb[~fb.consumer_id.isin(foreclosure_ids) & (fb.foreclosure_flag==1)]
    
    bankruptcies_ids = bankruptcies_ids.append(nb.consumer_id)
    foreclosure_ids = foreclosure_ids.append(nf.consumer_id)

    # calculate and append
    row = pd.Series([nb.bankruptcy_flag.sum(), nf.foreclosure_flag.sum()])
    row.index = ['bankruptcy_new', 'foreclosure_new']
    rowlist.append(row)    
    
    
    ####################################
    # PAGE 18: THIRD PARTY COLLECTIONS #
    ####################################
    
    # subset to relevant variables (dropping duplicates bc this is consumer level information)
    tpc_vars = ['consumer_id', 'number_3rd_party_collection_accts','total_amount_3rd_party_collections']
    tpc = mon1[tpc_vars].drop_duplicates()

    # clean exception code variables 
    tpc.loc[tpc['number_3rd_party_collection_accts']>=92, 'number_3rd_party_collection_accts'] = 0

    # we just want to know whether this person has third part collections, not how many accounts 
    tpc.loc[tpc['number_3rd_party_collection_accts']>=1, 'number_3rd_party_collection_accts'] = 1

    # clean exception code variables 
    tpc.loc[tpc['total_amount_3rd_party_collections']>=9999992, 'total_amount_3rd_party_collections'] = np.NaN

    # we want to know the average collection amount ONLY for people who have collections 
    tpc.loc[tpc['total_amount_3rd_party_collections']==0, 'total_amount_3rd_party_collections'] = np.NaN
    
    # look at the averages
    row = tpc[['number_3rd_party_collection_accts','total_amount_3rd_party_collections']].mean()
    
    # rename index and append 
    row.index = ["perc_3rd_party_collection_accts", "total_amount_3rd_party_collections"]
    rowlist.append(row)
    
    ########################################
    # PAGE 18 ALT: THIRD PARTY COLLECTIONS #
    ########################################
    
    # filter for certain variables 
    tpc_alt_vars = ['consumer_id', 'status_category', 'balance']
    tpc2 = mon1[tpc_alt_vars]
    
    # total number of people
    n_people = len(tpc2.consumer_id.unique())
    
    # filter for all the 120+ day DPD or in collections tradelines
    tpc2 = tpc2[tpc2.status_category == 5]
    
    # count the number of people
    dpd_coll = len(tpc2.consumer_id.unique())
    
    # find the average balance
    avg_tpc_bal = tpc2.groupby('consumer_id').sum().balance.mean()
    
    row = pd.Series([dpd_coll/n_people, avg_tpc_bal])
    row.index = ['coll_perc', 'coll_amt']
    
    rowlist.append(row)
    
    print(row)
    
    ##########################################
    # PAGE 20: TOTAL DEBT COMPOSITION BY AGE #
    ##########################################
    
    # get relevant variables
    age_vars = ['consumer_age_archive', 'product_category', 'balance']
    age = mon1[age_vars]
    
    # aggregate by age 
    bal_by_age = age.groupby('consumer_age_archive').sum()
    
    # format index 
    bal_by_age.index = ['Unknown',
                    '18–24',
                    '25–34',
                    '35-44',
                    '45-54',
                    '55–64',
                    '65–74',
                    '75+'] 
    bal_by_age.index = bal_by_age.index + '_total'
    
    # append
    rowlist.append(bal_by_age)
    
    
    #############################
    # PAGE 21: DEBT SHARE BY PRODUCT TYPE AND AGE--ONLY FOR THE LAST QUARTER
    ####################   
    
    if(quarternum == end): 
        
        # calculate balance by type and age 
        bal_by_type_and_age = age.groupby(['consumer_age_archive', 'product_category']).sum()
        
        # merge in the total balance information 
        all_age = bal_by_type_and_age.join(age.groupby('consumer_age_archive').sum(), rsuffix="_all")
        all_age['perc'] = all_age.balance/all_age.balance_all
        
        # create final pivot table
        final_age_by_type = pd.pivot_table(all_age.reset_index(), values = 'perc', index = 'consumer_age_archive', columns = 'product_category')
    
    
    ###################################################
    # PAGE 32: TOTAL DEBT BALANCE PER CAPITA BY STATE #
    ###################################################
    
    # relevant variables and states 
    pc_vars = ['consumer_id', 'state', 'product_category', 'balance']
    states = ['IL', 'NJ', 'TX', 'OH', 'PA', 'FL', 'MI', 'NV', 'CA', 'NY', 'AZ']

    # subset to relevant variables 
    pc = mon1[pc_vars]
    
    # calculate national per capital debt balance and append 
    totalnat = pc.groupby('consumer_id').sum().mean()
    totalnat.index = ['national_per_capita']
    rowlist.append(totalnat)
    
    # State level time! Let's only pick the tradelines in states that we want 
    states = pc[pc['state'].isin(states)]

    # number of consumers in each state
    count_states = states[['state', 'consumer_id']].drop_duplicates().groupby('state').count()
    
    # total debt balance in each state
    state_pc = states[['state', 'balance']].groupby(['state']).sum()/count_states.to_numpy()

    # total debt balance in each state, broken out by type of debt 
    state_byperson = states.drop('consumer_id', axis = 1).groupby(['state', 'product_category']).sum()
    
    # state per capita export
    state_pc.index = state_pc.index + "_per_capita"
    rowlist.append(state_pc)
    
    
    #############################
    # PAGE 33: DEBT COMPOSITION PER CAPITA--ONLY FOR THE LAST QUARTER
    ####################
    
    if (quarternum == end):
        
        # per capita by type of debt by state
        state_comb = state_byperson.join(count_states).reset_index()
        state_comb['per_capita'] = state_comb.balance/state_comb.consumer_id

        final_per_capita = pd.pivot_table(state_comb, values='per_capita', index='state', columns='product_category')    

    
    ##########################################
    # CREATING THE FINAL ROW FOR THE QUARTER #
    ##########################################

    finalrow = pd.concat(rowlist)
    
    # doing some more funky shaping 
    finalrow[DATE] = finalrow.sum(axis=1)
    finalrow = finalrow[DATE]
    
    # add the final row to the list of all the rows
    allrowlist.append(finalrow)
    
    # print our progress pls
    print(f"Done with quarter {quarternum}: {DATE}", flush = True)
    

#################
# CONCATENATION #
#################

# finally, creating the csv for all the months
final_stats = pd.concat(allrowlist, axis = 1).T

print("WE APPENDED ALL THE ROWS TOGETHER", flush = True)

#############
# EXPORTING #
#############

outfile_str = output_4 + "3_ny_fed_other_stats.csv"
final_stats.to_csv(outfile_str)

outfile_per_capita = output_4 + "3_debt_by_type_per_capita.csv"
final_per_capita.to_csv(outfile_per_capita)

outfile_age = output_4 + "3_debt_by_age_by_type.csv"
final_age_by_type.to_csv(outfile_age)

print("WE DID IT!!!!!!!!111!!1111!1ONEONE")