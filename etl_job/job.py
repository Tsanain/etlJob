import boto3
import json
import pandas as pd
import numpy as np
import warnings
from datetime import timezone, datetime


warnings.filterwarnings("ignore")

# bucket names, input file names
bucket, dion, sawyer, product_master = "tsan-etljob-src", "Dion-DISTRIBUTOR_R3_MASTER-2022-07-04_19_00_00.json", "Sawyer-DISTRIBUTOR_R3_MASTER-2022-07-04_19_00_00.json", "Dion-PRODUCT_MASTER_DISTRIBUTOR_OUTPUT-yyyy-mm-dd_19_00_00.json"
dest_bucket = "tsan-etljob-dest"

# s3 sdk
s3_obj = boto3.resource('s3')
s3_client = boto3.client('s3')

# function to get dataframe from s3 bucket


def getDF_from_S3(bucketName, fileName):

    res = s3_client.get_object(Bucket=bucketName, Key=fileName)
    data = res['Body'].read()
    data = json.loads(data)
    df = pd.DataFrame(data['data'])
    return df

# function to write json file to s3 bucket


def putDf_To_S3(bucket, filename, df):

    output = {}
    output['data'] = df.to_dict(orient='records')
    object = s3_obj.Object(bucket, filename)
    result = object.put(Body=json.dumps(output))
    return result

# function to print column names and count


def printCol(df):
    i = 0
    for col in df.columns:
        print(col)
        i += 1
    print("count:", i)

# function to compare 2 dataframes and print uncommon column names


def compCol(a, b):
    ans = []
    flag = 0
    for col in a.columns:
        for col2 in b.columns:
            if (col == col2):
                flag = 1
                break
            else:
                flag = 0

        if (flag == 0):
            ans.append(col)
    return ans


# get required inputs
dion_df = getDF_from_S3(bucket, dion)
# about 200 rows less, and cols with (n), in the other file 1000 rows extra and many cols absent.
product_master_df = getDF_from_S3(bucket, product_master)

'''---------- IMPORT: SP R3 MASTER -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

sawyer_df = getDF_from_S3(bucket, sawyer)

'''---------- IMPORT: DS R3 MASTER FILE  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

dion_df['COMPONENT 2,3,4'] = np.where(dion_df.DISTRIBUTOR_COMPONENT2_ITEM_ID != "", "remove",
                                      np.where(dion_df.DISTRIBUTOR_COMPONENT3_ITEM_ID != "", "remove",
                                               np.where(dion_df.DISTRIBUTOR_COMPONENT2_ITEM_ID != "", "remove", "")))

output = dion_df[dion_df['COMPONENT 2,3,4'] == "remove"]

res = putDf_To_S3(
    dest_bucket, "Exceptions: R3 Master Populated On Component 2,3 and 4.json", output)

dion_df_copy = dion_df[dion_df['COMPONENT 2,3,4'] != "remove"]

dion_df.drop(columns=['COMPONENT 2,3,4'], inplace=True)

dion_df_copy.drop(columns=['COMPONENT 2,3,4'], inplace=True)

# "%2f", "/"" can not use either of them, "-" used instead.
res = putDf_To_S3(
    dest_bucket, "Output: Import - Append - R3 Table.json", dion_df_copy)

'''---------- MIRCO DE-PIVOT R3 TABLE   ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''
# MUTED on paxata

# product_master_df_copy = product_master_df[['DISTRIBUTOR_ITEM_STATUS_DESCRIPTION', 'DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_SALEABLE_PRODUCT_DESCRIPTION', 'DISTRIBUTOR_GALLON_CONVERSION_FACTOR', 'DISTRIBUTOR_PACK_COUNT_DESCRIPTION', 'DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT',
#                                             'DISTRIBUTOR_SALES_CODE_ID', 'DISTRIBUTOR_COST_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_PRICE_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_REPORTING_ITEM_ID', 'DISTRIBUTOR_INVENTORY_TYPE', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE', 'DISTRIBUTOR_SHIPPABLE_PRODUCT_GROUP']]

# lookup1 = pd.merge(dion_df, product_master_df_copy, on='DISTRIBUTOR_ITEM_ID')

# lookup1_copy = lookup1

# lookup1 = lookup1.iloc[:, 0:17]

# product_master_df_copy['DISTRIBUTOR_PACK_COUNT_DESCRIPTION'] = product_master_df_copy.DISTRIBUTOR_PACK_COUNT_DESCRIPTION.astype(
#     str).astype(float)

# lookup1['newCol'] = lookup1.DISTRIBUTOR_COMPONENT1_QUANTITY * product_master_df_copy.DISTRIBUTOR_PACK_COUNT_DESCRIPTION

# lookup1['newCol2'] = np.where(lookup1['DISTRIBUTOR_COMPONENT1_QUANTITY'] ==
#                               lookup1_copy['DISTRIBUTOR_GALLON_CONVERSION_FACTOR'], 'Match', 'Do Not Match')

# #rows to be removed and col to be renamed, steps not opening.
# res = putDf_To_S3(
#     dest_bucket, 'R3 Exceptions: Component1 Item Quantity and Conversion Factor Do Not Match.json', lookup1)

res = putDf_To_S3(
    dest_bucket, 'R3 Exceptions: Component1 Item Quantity and Conversion Factor Do Not Match.json', dion_df)


depivoted_df = pd.melt(dion_df, id_vars=['DISTRIBUTOR_R3_TYPE', 'DISTRIBUTOR_ITEM_ID', 'RECORD_TYPE', 'DISTRIBUTOR_COMPONENT1_QUANTITY', 'DISTRIBUTOR_WAREHOUSE_ID', 'DISTRIBUTOR_COMPONENT2_QUANTITY', 'DISTRIBUTOR_COMPONENT3_QUANTITY', 'DISTRIBUTOR_COMPONENT4_QUANTITY', 'DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPONENT2_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPONENT3_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPONENT4_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPANY_ID'],
                       value_vars=['DISTRIBUTOR_COMPONENT1_ITEM_ID', 'DISTRIBUTOR_COMPONENT2_ITEM_ID',
                                   'DISTRIBUTOR_COMPONENT3_ITEM_ID', 'DISTRIBUTOR_COMPONENT4_ITEM_ID'],
                       var_name='REFERENCE', value_name='COMPONENT_COMPUTED'
                       )

depivoted_df['COMPONENT_COMPUTED'].replace('', np.nan, inplace=True)

depivoted_df.dropna(subset=['COMPONENT_COMPUTED'], inplace=True)


depivoted_df['UNIT_OF_MEASUREMENT(COMPUTED)'] = np.where(depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT1_ITEM_ID', depivoted_df.DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT,
                                                         np.where(depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT2_ITEM_ID', depivoted_df.DISTRIBUTOR_COMPONENT2_UNIT_OF_MEASUREMENT,
                                                                  np.where(depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT3_ITEM_ID', depivoted_df.DISTRIBUTOR_COMPONENT3_UNIT_OF_MEASUREMENT,
                                                                           np.where(depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT4_ITEM_ID', depivoted_df.DISTRIBUTOR_COMPONENT4_UNIT_OF_MEASUREMENT, ''
                                                                                    ))))

depivoted_df['COMPONENT_QUANTITY(COMPUTED)'] = np.where(depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT1_ITEM_ID', depivoted_df.DISTRIBUTOR_COMPONENT1_QUANTITY,
                                                        np.where(depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT2_ITEM_ID', depivoted_df.DISTRIBUTOR_COMPONENT2_QUANTITY,
                                                                 np.where(depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT3_ITEM_ID', depivoted_df.DISTRIBUTOR_COMPONENT3_QUANTITY,
                                                                          np.where(depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT4_ITEM_ID', depivoted_df.DISTRIBUTOR_COMPONENT4_QUANTITY, ''
                                                                                   ))))


depivoted_df = depivoted_df.drop(columns=['DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPONENT2_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPONENT3_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPONENT4_UNIT_OF_MEASUREMENT',
                                          'DISTRIBUTOR_COMPONENT1_QUANTITY', 'DISTRIBUTOR_COMPONENT2_QUANTITY', 'DISTRIBUTOR_COMPONENT3_QUANTITY', 'DISTRIBUTOR_COMPONENT4_QUANTITY'])


product_master_df_copy2 = product_master_df[['DISTRIBUTOR_ITEM_STATUS_DESCRIPTION', 'DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_ITEM_DESCRIPTION', 'DISTRIBUTOR_GALLON_CONVERSION_FACTOR', 'MANUFACTURER_NAME', 'DISTRIBUTOR_SALES_CODE_ID', 'DISTRIBUTOR_COST_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_PRICE_UNIT_OF_MEASUREMENT',
                                            #'WARNING: BLANK DISTRIBUTOR PRICE UNIT OF MEASUREMENT',
                                             # 'WARNING: BLANK DISTRIBUTOR COST UNIT OF MEASUREMENT', 'WARNING: STANDARD COST IS GREATER THAN SELL PRICE', #not in file
                                             # (2) diff versions exist at many cols
                                             'DISTRIBUTOR_REPORTING_ITEM_ID', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE',
                                             # 'Units per layer' not in json file.
                                             'DISTRIBUTOR_PACK_DESCRIPTION', 'DISTRIBUTOR_UNITS_PER_LAYER'
                                             ]]

lookup2 = pd.merge(depivoted_df, product_master_df_copy2,
                   on='DISTRIBUTOR_ITEM_ID', how="left")  # some cols are missing

output1 = lookup2.drop(columns=['DISTRIBUTOR_GALLON_CONVERSION_FACTOR', 'MANUFACTURER_NAME',
                       'DISTRIBUTOR_COST_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_PRICE_UNIT_OF_MEASUREMENT', ])

output1.rename(columns={'Columns': 'REFERENCE', 'COMPONENT_COMPUTED': 'DISTRIBUTOR_COMPONENT1_ITEM_ID', 'UNIT_OF_MEASUREMENT(COMPUTED)':
               'DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT', 'COMPONENT_QUANTITY(COMPUTED)': 'DISTRIBUTOR_COMPONENT1_QUANTITY', 'DISTRIBUTOR_ITEM_DESCRIPTION': 'DISTRIBUTOR_ITEM_DESCRIPTION (DISTRIBUTOR_ITEM_ID)'}, inplace=True)

res = putDf_To_S3(
    dest_bucket, 'Output: R3 Table Before Sales Code Removal Step.json', output1)

# # MUTED on paxata
# #output2 = output1[output1['DISTRIBUTOR_SALES_CODE_ID'] != '4']

# #output2.rename(columns={'Columns': 'REFERENCE', 'COMPONENT_COMPUTED': 'DISTRIBUTOR_COMPONENT1_ITEM_ID', 'UNIT_OF_MEASUREMENT(COMPUTED)':
#                'DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT', 'COMPONENT_QUANTITY(COMPUTED)': 'DISTRIBUTOR_COMPONENT1_QUANTITY'}, inplace=True)


# res = putDf_To_S3(
#     dest_bucket, 'Output: Micro - De-Pivot R3 Table.json', output2)

res = putDf_To_S3(
    dest_bucket, 'Output: Micro - De-Pivot R3 Table.json', output1)


product_master_df_copy3 = product_master_df[['DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_ITEM_DESCRIPTION', 'MANUFACTURER_NAME', 'DISTRIBUTOR_COST_UNIT_OF_MEASUREMENT',
                                            # (3)', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE (1)'
                                             # 'Units per layer' not in json file.
                                             'DISTRIBUTOR_PRICE_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_REPORTING_ITEM_ID', 'DISTRIBUTOR_INVENTORY_TYPE', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE', 'DISTRIBUTOR_UNITS_PER_LAYER'
                                             ]]

product_master_df_copy3 = product_master_df_copy3.rename(
    columns={'DISTRIBUTOR_ITEM_ID': 'DISTRIBUTOR_ITEM_ID (1)', 'DISTRIBUTOR_ITEM_DESCRIPTION': 'DISTRIBUTOR_ITEM_DESCRIPTION (COMPONENT1)', 'MANUFACTURER_NAME': 'MANUFACTURER_NAME (COMPONENT1)'})

lookup3 = pd.merge(output1, product_master_df_copy3,
                   left_on='DISTRIBUTOR_COMPONENT1_ITEM_ID', right_on='DISTRIBUTOR_ITEM_ID (1)', how="left")


lookup3 = lookup3.drop(
    columns=['DISTRIBUTOR_WAREHOUSE_ID', 'DISTRIBUTOR_INVENTORY_TYPE'])

df = lookup3.groupby(['RECORD_TYPE', 'DISTRIBUTOR_ITEM_ID']).agg(COUNT_DISTINCT_DISTRIBUTOR_COMPONENT1_ITEM_ID=pd.NamedAgg(
    column='DISTRIBUTOR_COMPONENT1_ITEM_ID', aggfunc="nunique")).reset_index()

df = df[df['COUNT_DISTINCT_DISTRIBUTOR_COMPONENT1_ITEM_ID'] != 2]


res = putDf_To_S3(
    dest_bucket, "Output: Unique Source Item ID per Repack Item.json", df)

'''---------- MICRO DEPIVOT SP TABLE -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

depivoted_sp_df = pd.melt(sawyer_df, id_vars=['RECORD_TYPE', 'DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_R3_TYPE', 'DISTRIBUTOR_COMPONENT1_QUANTITY', 'DISTRIBUTOR_COMPONENT2_QUANTITY', 'DISTRIBUTOR_COMPONENT3_QUANTITY', 'DISTRIBUTOR_COMPONENT4_QUANTITY', 'DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPONENT2_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPONENT3_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPONENT4_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPANY_ID', 'DISTRIBUTOR_WAREHOUSE_ID'],
                          value_vars=['DISTRIBUTOR_COMPONENT1_ITEM_ID', 'DISTRIBUTOR_COMPONENT2_ITEM_ID',
                                      'DISTRIBUTOR_COMPONENT3_ITEM_ID', 'DISTRIBUTOR_COMPONENT4_ITEM_ID'],
                          var_name='Columns', value_name='COMPONENT_COMPUTED'
                          )


depivoted_sp_df['COMPONENT_COMPUTED'].replace('', np.nan, inplace=True)

depivoted_sp_df.dropna(subset=['COMPONENT_COMPUTED'], inplace=True)


depivoted_sp_df['UNIT_OF_MEASUREMENT(COMPUTED)'] = np.where(depivoted_sp_df.Columns == 'DISTRIBUTOR_COMPONENT1_ITEM_ID', depivoted_sp_df.DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT,
                                                            np.where(depivoted_sp_df.Columns == 'DISTRIBUTOR_COMPONENT2_ITEM_ID', depivoted_sp_df.DISTRIBUTOR_COMPONENT2_UNIT_OF_MEASUREMENT,
                                                                     np.where(depivoted_sp_df.Columns == 'DISTRIBUTOR_COMPONENT3_ITEM_ID', depivoted_sp_df.DISTRIBUTOR_COMPONENT3_UNIT_OF_MEASUREMENT,
                                                                              np.where(depivoted_sp_df.Columns == 'DISTRIBUTOR_COMPONENT4_ITEM_ID', depivoted_sp_df.DISTRIBUTOR_COMPONENT4_UNIT_OF_MEASUREMENT, ''
                                                                                       ))))


depivoted_sp_df['COMPONENT_QUANTITY(COMPUTED)'] = np.where(depivoted_sp_df.Columns == 'DISTRIBUTOR_COMPONENT1_ITEM_ID', depivoted_sp_df.DISTRIBUTOR_COMPONENT1_QUANTITY,
                                                           np.where(depivoted_sp_df.Columns == 'DISTRIBUTOR_COMPONENT2_ITEM_ID', depivoted_sp_df.DISTRIBUTOR_COMPONENT2_QUANTITY,
                                                                    np.where(depivoted_sp_df.Columns == 'DISTRIBUTOR_COMPONENT3_ITEM_ID', depivoted_sp_df.DISTRIBUTOR_COMPONENT3_QUANTITY,
                                                                             np.where(depivoted_sp_df.Columns == 'DISTRIBUTOR_COMPONENT4_ITEM_ID', depivoted_sp_df.DISTRIBUTOR_COMPONENT4_QUANTITY, ''
                                                                                      ))))


depivoted_sp_df.drop(columns=['DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPONENT2_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPONENT3_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPONENT4_UNIT_OF_MEASUREMENT',
                              'DISTRIBUTOR_COMPONENT1_QUANTITY', 'DISTRIBUTOR_COMPONENT2_QUANTITY', 'DISTRIBUTOR_COMPONENT3_QUANTITY', 'DISTRIBUTOR_COMPONENT4_QUANTITY'], inplace=True)

depivoted_sp_df.rename(columns={'Columns': 'REFERENCE', 'COMPONENT_COMPUTED': 'DISTRIBUTOR_COMPONENT1_ITEM_ID', 'UNIT_OF_MEASUREMENT(COMPUTED)':
                       'DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT', 'COMPONENT_QUANTITY(COMPUTED)': 'DISTRIBUTOR_COMPONENT1_QUANTITY'}, inplace=True)

product_master_df_copy4 = product_master_df[['DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_SALES_CODE_ID', 'DISTRIBUTOR_REPORTING_ITEM_ID',
                                             'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE', 'DISTRIBUTOR_SHIPPABLE_PRODUCT_GROUP', 'DISTRIBUTOR_PACK_DESCRIPTION', 'DISTRIBUTOR_UNITS_PER_LAYER']]

lookup4 = pd.merge(depivoted_sp_df, product_master_df_copy4,
                   on='DISTRIBUTOR_ITEM_ID', how="left")

lookup4.drop(columns=['DISTRIBUTOR_SHIPPABLE_PRODUCT_GROUP'], inplace=True)

res = putDf_To_S3(
    dest_bucket, 'Output: SP R3 Table Before Sales Code Removal Step.json', lookup4)

#Muted in paxata
#lookup4_copy = lookup4[lookup4['DISTRIBUTOR_SALES_CODE_ID'] != '4']

res = putDf_To_S3(
    dest_bucket, 'Output: Micro - De-Pivot SP R3 Table.json', lookup4)

product_master_df_copy5 = product_master_df[['DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_ITEM_DESCRIPTION', 'MANUFACTURER_NAME',
                                             'DISTRIBUTOR_REPORTING_ITEM_ID', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE', 'DISTRIBUTOR_UNITS_PER_LAYER']]

product_master_df_copy5.rename(columns={'DISTRIBUTOR_ITEM_DESCRIPTION': 'DISTRIBUTOR_ITEM_DESCRIPTION (DISTRIBUTOR_ITEM_ID)',
                               'MANUFACTURER_NAME': 'MANUFACTURER_NAME (COMPONENT1)', 'DISTRIBUTOR_ITEM_ID': 'DISTRIBUTOR_ITEM_ID(1)'}, inplace=True)

lookup5 = pd.merge(lookup4, product_master_df_copy5,
                   left_on='DISTRIBUTOR_COMPONENT1_ITEM_ID', right_on='DISTRIBUTOR_ITEM_ID(1)', how="left")

lookup5.drop(columns=['DISTRIBUTOR_WAREHOUSE_ID'])


df = lookup5.groupby(['RECORD_TYPE', 'DISTRIBUTOR_ITEM_ID']).agg(COUNT_DISTINCT_DISTRIBUTOR_COMPONENT1_ITEM_ID=pd.NamedAgg(
    column='DISTRIBUTOR_COMPONENT1_ITEM_ID', aggfunc="nunique")).reset_index()

res = putDf_To_S3(
    dest_bucket, "Output: SP Unique Source Item ID per Repack Item.json", df)

'''---------- MICRO: R3 FOR PRODUCT MASTER JOIN  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

df_start = getDF_from_S3(
    dest_bucket, 'Output: R3 Table Before Sales Code Removal Step.json')

df_import = getDF_from_S3(
    dest_bucket, 'Output: SP R3 Table Before Sales Code Removal Step.json')


# Is this the way to append 2 dataframes.....

df = pd.concat([df_start, df_import], axis=0, ignore_index=True)


# diff in rows: 100
df = df[df['DISTRIBUTOR_COMPANY_ID'] != 'SP']

# diff in rows: 57

df.drop(columns=['DISTRIBUTOR_COMPANY_ID', 'DISTRIBUTOR_PACK_DESCRIPTION',
        'DISTRIBUTOR_REPORTING_ITEM_ID', 'DISTRIBUTOR_UNITS_PER_LAYER'], inplace=True)

# diff in cols is: DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE(n)

res = putDf_To_S3(
    dest_bucket, 'Output: R3 Table for Product Master Join.json', df)

'''------- JOIN: UNIQUE SP SOURCE ITEM PER COMPONENT ITEM ID  --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

df_start = getDF_from_S3(
    dest_bucket, 'Output: Micro - De-Pivot SP R3 Table.json')

df_import = getDF_from_S3(
    dest_bucket, 'Output: SP Unique Source Item ID per Repack Item.json')

df = pd.merge(df_start, df_import, on="DISTRIBUTOR_ITEM_ID", how="left")

df.drop(columns=['REFERENCE', 'DISTRIBUTOR_SALES_CODE_ID', 'DISTRIBUTOR_PACK_DESCRIPTION',
        'DISTRIBUTOR_UNITS_PER_LAYER', 'RECORD_TYPE_y', 'COUNT_DISTINCT_DISTRIBUTOR_COMPONENT1_ITEM_ID'], inplace=True)
df.rename(columns={'RECORD_TYPE_x': 'RECORD_TYPE'}, inplace=True)

res = putDf_To_S3(
    dest_bucket, 'Output: Join - Unique SP Source Item per Component Item ID.json', df)

'''-------- IMPORT PRODUCT MASTER ---upper  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

product_master_df_copy = product_master_df.drop(columns=['DISTRIBUTOR_REPORTING_ITEM_ID', 'DISTRIBUTOR_INVENTORY_TYPE',
                                                'DISTRIBUTOR_SHIPPABLE_PRODUCT_GROUP', 'DISTRIBUTOR_PACK_DESCRIPTION', 'DISTRIBUTOR_UNITS_PER_LAYER'])

res = putDf_To_S3(
    dest_bucket, "Output: Import - Product Master.json", product_master_df_copy)

'''------- JOIN: UNIQUE SOURCE ITEM PER COMPONENT ITEM ID  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

df_start = getDF_from_S3(dest_bucket, "Output: Micro - De-Pivot R3 Table.json")


df_import = getDF_from_S3(
    dest_bucket, "Output: Unique Source Item ID per Repack Item.json")

df = pd.merge(df_start, df_import, on='DISTRIBUTOR_ITEM_ID')

# On what column to remove rows -> remove row which have nan values for item_id -> join is done on item_id, how can it increase.....
# print(df[df['DISTRIBUTOR_ITEM_ID'] == '']) -> empty dataframe, so no mull values.

df.drop(columns=['REFERENCE', 'DISTRIBUTOR_ITEM_STATUS_DESCRIPTION', 'DISTRIBUTOR_ITEM_DESCRIPTION (DISTRIBUTOR_ITEM_ID)', 'DISTRIBUTOR_SALES_CODE_ID',
        'DISTRIBUTOR_PACK_DESCRIPTION', 'DISTRIBUTOR_UNITS_PER_LAYER', 'COUNT_DISTINCT_DISTRIBUTOR_COMPONENT1_ITEM_ID', 'RECORD_TYPE_y'], inplace=True)
df.rename(columns={'RECORD_TYPE_x': 'RECORD_TYPE'}, inplace=True)

res = putDf_To_S3(
    dest_bucket, 'Output: Join - Unique Source Item per Component Item ID.json', df)

'''
------ IMPORT PRODUCT MASTER ---- lower  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#OUT_OF_SCOPE_RECORDS col is missing.
product_master_df_copy = product_master_df.drop(columns=['DISTRIBUTOR_REPORTING_ITEM_ID', 'DISTRIBUTOR_INVENTORY_TYPE', 'DISTRIBUTOR_SHIPPABLE_PRODUCT_GROUP', 'DISTRIBUTOR_PACK_DESCRIPTION', 'DISTRIBUTOR_UNITS_PER_LAYER'])

res = putDf_To_S3(dest_bucket, "Output: Import - Product Master.json", product_master_df_copy)
'''

'''------ JOIN: PRODUCT MASTER TO COMPUTE CORRECT ITEM QUANTITY  --- upper ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

df_start = getDF_from_S3(
    dest_bucket, "Output: Join - Unique SP Source Item per Component Item ID.json")
df_import = getDF_from_S3(dest_bucket, "Output: Import - Product Master.json")

df_import = df_import[['DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_PACK_COUNT_DESCRIPTION',
                      'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE']]  # , 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE (2)', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE (1)', 'EXCEPTIONS']]

df = pd.merge(df_start, df_import, on="DISTRIBUTOR_ITEM_ID", how="left")

df.DISTRIBUTOR_COMPONENT1_QUANTITY = df.DISTRIBUTOR_COMPONENT1_QUANTITY.astype(
    float)


df['DISTRIBUTOR_PACK_COUNT_DESCRIPTION'] = df.DISTRIBUTOR_PACK_COUNT_DESCRIPTION.astype(
    str).astype(float)

df['New_Column'] = df.DISTRIBUTOR_COMPONENT1_QUANTITY * \
    df.DISTRIBUTOR_PACK_COUNT_DESCRIPTION

df.rename(columns={'DISTRIBUTOR_COMPONENT1_QUANTITY': 'DISTRIBUTOR_COMPONENT1_CONVERSION_QUANTITY',
          'New_Column': 'DISTRIBUTOR_COMPONENT1_QUANTITY'}, inplace=True)
df.drop(columns=['DISTRIBUTOR_PACK_COUNT_DESCRIPTION'], inplace=True)

product_master_df_copy = product_master_df[['DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_ITEM_DESCRIPTION',
                                            'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE', 'DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT']]

product_master_df_copy.rename(columns={
                              'DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT': 'DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT (repack)'}, inplace=True)

lookup1 = pd.merge(df, product_master_df_copy, on='DISTRIBUTOR_ITEM_ID')

product_master_df_copy2 = product_master_df[[
    'DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE']]

product_master_df_copy2.rename(columns={'DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT': 'DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT (Source)',
                               'DISTRIBUTOR_ITEM_ID': 'DISTRIBUTOR_ITEM_ID (Source)'}, inplace=True)

# DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE has 10 versions....

lookup2 = pd.merge(lookup1, product_master_df_copy2, left_on="DISTRIBUTOR_COMPONENT1_ITEM_ID",
                   right_on='DISTRIBUTOR_ITEM_ID (Source)', how="left")

lookup2['EXCEPTION: BASE UOM IS NOT "GAL" FOR COMPONENT QUANTITY 1'] = np.where(
    lookup2['DISTRIBUTOR_COMPONENT1_QUANTITY'] == "1",  np.where(lookup2['DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT'] != "GAL", "R3M06", ""), "")

lookup2.drop(columns=['DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT (repack)',
             'DISTRIBUTOR_ITEM_ID (Source)', 'DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT (Source)'], inplace=True)

res = putDf_To_S3(
    dest_bucket, 'Output: Join Product Master to Compute Correct Item Quantity (upper).json', lookup2)


'''-------JOIN: PRODUCT MASTER TO COMPUTE CORRECT ITEM QUANTITY lower ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

df_start = getDF_from_S3(
    dest_bucket, 'Output: Join - Unique Source Item per Component Item ID.json')

df_import = getDF_from_S3(dest_bucket, 'Output: Import - Product Master.json')

df_import = df_import[['DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_PACK_COUNT_DESCRIPTION',
                      'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE', 'DISTRIBUTOR_COST_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_PRICE_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE']]

lookup1 = pd.merge(df_start, df_import, on='DISTRIBUTOR_ITEM_ID', how="left")

lookup1['DISTRIBUTOR_COMPONENT1_QUANTITY'] = lookup1.DISTRIBUTOR_COMPONENT1_QUANTITY.astype(
    float)

lookup1['DISTRIBUTOR_PACK_COUNT_DESCRIPTION'] = lookup1.DISTRIBUTOR_PACK_COUNT_DESCRIPTION.astype(
    float)

lookup1['New_Column'] = lookup1.DISTRIBUTOR_COMPONENT1_QUANTITY * \
    lookup1.DISTRIBUTOR_PACK_COUNT_DESCRIPTION

lookup1.rename(columns={'DISTRIBUTOR_COMPONENT1_QUANTITY': 'DISTRIBUTOR_COMPONENT1_CONVERSION_QUANTITY',
               'New_Column': 'DISTRIBUTOR_COMPONENT1_QUANTITY'}, inplace=True)

lookup1.drop(columns=['DISTRIBUTOR_PACK_COUNT_DESCRIPTION'], inplace=True)

df_import2 = getDF_from_S3(dest_bucket, 'Output: Import - Product Master.json')

df_import2 = df_import2[['DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_ITEM_DESCRIPTION', 'DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT',
                         'DISTRIBUTOR_COST_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_PRICE_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE']]

df_import2.rename(columns={
                  'DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT': 'DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT (repack)'}, inplace=True)

lookup2 = pd.merge(lookup1, df_import2, on='DISTRIBUTOR_ITEM_ID', how="left")

df_import3 = getDF_from_S3(dest_bucket, 'Output: Import - Product Master.json')

df_import3 = df_import3[['DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT',
                         'DISTRIBUTOR_COST_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_PRICE_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE']]
df_import3.rename(columns={'DISTRIBUTOR_ITEM_ID': 'DISTRIBUTOR_ITEM_ID (Source)',
                  'DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT': 'DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT (Source)'}, inplace=True)

lookup3 = pd.merge(lookup2, df_import3, left_on='DISTRIBUTOR_COMPONENT1_ITEM_ID',
                   right_on='DISTRIBUTOR_ITEM_ID (Source)', how="left")

lookup3['EXCEPTION: BASE UOM IS NOT "GAL" FOR COMPONENT QUANTITY 1'] = np.where(
    lookup3['DISTRIBUTOR_COMPONENT1_QUANTITY'] == "1",  np.where(lookup3['DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT'] != "GAL", "R3M06", ""), "")

# DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE has 10 versions in form of _x _y. How to resolve this....

lookup3.drop(columns=['DISTRIBUTOR_COST_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_PRICE_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT (Source)',
                      'DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT (repack)', 'DISTRIBUTOR_ITEM_ID (Source)'], inplace=True)

# same file name as above step....

res = putDf_To_S3(
    dest_bucket, 'Output: Join Product Master to Compute Correct Item Quantity (lower).json', lookup3)

'''-------JOIN: DS AND SP R3 TABLES ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

df_start = getDF_from_S3(
    dest_bucket, 'Output: Join Product Master to Compute Correct Item Quantity (upper).json')

df_import = getDF_from_S3(
    dest_bucket, 'Output: Join Product Master to Compute Correct Item Quantity (lower).json')

# Doubt: is it okay to drop dupliactes here? else number of rows was going 1762+
df_import.drop_duplicates(subset="DISTRIBUTOR_ITEM_ID", inplace=True)

df_import.rename(
    columns={"DISTRIBUTOR_ITEM_ID": "DISTRIBUTOR_ITEM_ID (1)"}, inplace=True)

lookup1 = pd.merge(df_start, df_import, left_on='DISTRIBUTOR_ITEM_ID',
                   right_on='DISTRIBUTOR_ITEM_ID (1)', how="left")

lookup1 = lookup1[lookup1['DISTRIBUTOR_ITEM_ID (1)'].isnull()]

res = putDf_To_S3(dest_bucket, 'Output: SP R3 Table for Append.json', lookup1)

'''-------APPEND: DS AND SP R3 TABLES ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

df_start = getDF_from_S3(
    dest_bucket, 'Output: Join Product Master to Compute Correct Item Quantity (lower).json')

df_import = getDF_from_S3(dest_bucket, 'Output: SP R3 Table for Append.json')

df = pd.concat([df_start, df_import], axis=0, ignore_index=True)

# DOUBT in suffixes.

df.drop(columns=['DISTRIBUTOR_ITEM_DESCRIPTION', 'EXCEPTION: BASE UOM IS NOT "GAL" FOR COMPONENT QUANTITY 1', 'DISTRIBUTOR_COMPANY_ID', 'RECORD_TYPE_x', 'DISTRIBUTOR_R3_TYPE_x', 'DISTRIBUTOR_COMPANY_ID_x', 'DISTRIBUTOR_WAREHOUSE_ID_x',
        'DISTRIBUTOR_COMPONENT1_ITEM_ID_x', 'DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT_x', 'DISTRIBUTOR_COMPONENT1_CONVERSION_QUANTITY_x', 'EXCEPTION: BASE UOM IS NOT "GAL" FOR COMPONENT QUANTITY 1_x'], inplace=True)

res = putDf_To_S3(dest_bucket, 'Output: Append - DS and SP R3 Table.json', df)

'''---------- MICRO: VALIDATE ITEM ID -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

df_start = getDF_from_S3(
    dest_bucket, 'Output: Append - DS and SP R3 Table.json')

df_start['EXCEPTION: DISTRIBUTOR ITEM ID IS BLANK'] = np.where(
    df_start['DISTRIBUTOR_ITEM_ID'] == '', "R3M01", "")

res = putDf_To_S3(
    dest_bucket, 'Output: Micro - Validate Item ID.json', df_start)


'''------- MICRO: DISTRIBUTOR R3 TYPE -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

#df_start = getDF_from_S3(dest_bucket, 'Output: Micro - Validate Item ID.json')

df_start['EXCEPTION: R3 TYPE IS BLANK'] = np.where(
    df_start['DISTRIBUTOR_R3_TYPE'] == "", "R3M02", "")

res = putDf_To_S3(
    dest_bucket, 'Output: Micro - Distributor R3 Type.json', df_start)

'''------- MICRO: COMPONENT 1 ITEM ID -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

# Should I call getDF_from_S3 every time, I have the dataframe already.....

#df_start = getDF_from_S3(dest_bucket, 'Output: Micro - Distributor R3 Type.json')

df_start['EXCEPTION: COMPONENT1 ITEM ID IS BLANK'] = np.where(
    df_start['DISTRIBUTOR_COMPONENT1_ITEM_ID'] == "", "R3M03", "")

# should I upload each file individually or create a bigger step with all exceptions in 1

res = putDf_To_S3(
    dest_bucket, 'Output: Micro - Distributor Component1 Item ID.json', df_start)

'''---------- MICRO: COMPONENT 1 QUANTITY -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

#df_start = getDF_from_S3(dest_bucket, 'Output: Micro - Distributor Component1 Item ID.json')

df_start['EXCEPTION: COMPONENT1 QUANTITY IS BLANK'] = np.where(
    df_start['DISTRIBUTOR_COMPONENT1_QUANTITY'] == "", "R3M04", "")

res = putDf_To_S3(
    dest_bucket, 'Output: Micro - Component1 Quantity.json', df_start)

'''---------- MICRO: COMPONENT 1 UOM -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

#df_start = getDF_from_S3(dest_bucket, 'Output: Micro - Component1 Quantity.json')

df_start['EXCEPTION: COMPONENT1 UOM IS BLANK'] = np.where(
    df_start['DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT'] == "", "R3M05", "")

res = putDf_To_S3(dest_bucket, 'Output: Micro - Component1 UoM.json', df_start)

'''---------- MICRO: TAG RECORD TYPE -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

#df_start = getDF_from_S3(dest_bucket, 'Output: Micro - Component1 UoM.json')
# Muted in paxata
#df_start['RECORD_TYPE'] = "DISTRIBUTOR_R3_MASTER"

res = putDf_To_S3(
    dest_bucket, 'Output: Micro - Tag Record Type.json', df_start)

'''---------- MACRO: PUBLISH EXCEPTIONS -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

#df_start = getDF_from_S3(dest_bucket, 'Output: Micro - Tag Record Type.json')

df_start['ALL_EXCEPTIONS'] = df_start.agg(
    lambda x: f"{x['EXCEPTION: DISTRIBUTOR ITEM ID IS BLANK']} | {x['EXCEPTION: R3 TYPE IS BLANK']} | {x['EXCEPTION: COMPONENT1 ITEM ID IS BLANK']} | {x['EXCEPTION: COMPONENT1 QUANTITY IS BLANK']} | {x['EXCEPTION: COMPONENT1 UOM IS BLANK']}", axis=1)

df_start['ALL_EXCEPTIONS'] = df_start['ALL_EXCEPTIONS'].astype(str)

df_start['ALL_EXCEPTIONS'].replace(" |  |  |  | ", "", inplace=True)

df_start['EXCEPTION_REASON'] = df_start['ALL_EXCEPTIONS']

res = putDf_To_S3(
    dest_bucket, "Output: Macro - Publish Exceptions.json", df_start)

'''---------- IMPORT: TIMESTAMP FIX TABLE -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

ct = datetime.now(timezone.utc)

data = {'RECORD_TYPE': "DISTRIBUTOR_R3_MASTER", 'TIMESTAMP': ct}

df = pd.DataFrame(data, index=[0])

df['TIMESTAMP'] = df['TIMESTAMP'].astype(str)

res = putDf_To_S3(dest_bucket, "Output: Import - Timestamp Fix Table.json", df)

'''-------JOIN: TIMESTAMP FIX TABLE  --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

df_start = getDF_from_S3(
    dest_bucket, "Output: Macro - Publish Exceptions.json")

df_import = getDF_from_S3(
    dest_bucket, "Output: Import - Timestamp Fix Table.json")

df = pd.merge(df_start, df_import, on="RECORD_TYPE", how="left")

df.drop(columns=["ALL_EXCEPTIONS", "EXCEPTION_REASON"], inplace=True)

res = putDf_To_S3(dest_bucket, "Output: Join - Timestamp Fix Table.json", df)

'''-------OUTPUT: PUBLISH R3 MASTER  --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'''

df_start = getDF_from_S3(
    dest_bucket, "Output: Join - Timestamp Fix Table.json")

df_start['DISTRIBUTOR_WAREHOUSE_ID'] = df_start['DISTRIBUTOR_WAREHOUSE_ID'].astype(
    str)

df_start.drop(columns=["EXCEPTION: DISTRIBUTOR ITEM ID IS BLANK", "EXCEPTION: R3 TYPE IS BLANK", "EXCEPTION: COMPONENT1 ITEM ID IS BLANK", "EXCEPTION: COMPONENT1 QUANTITY IS BLANK", "EXCEPTION: COMPONENT1 UOM IS BLANK",
                       "DISTRIBUTOR_COMPONENT1_CONVERSION_QUANTITY", 'EXCEPTION: BASE UOM IS NOT "GAL" FOR COMPONENT QUANTITY 1_y'],
              inplace=True)

df_start['RECORD_NUMBER'] = df_start.index

# code to move record_number to first
cols = df_start.columns.to_list()

cols = cols[-1:] + cols[:-1]

df_start = df_start[cols]

print(df_start.shape)

res = putDf_To_S3(dest_bucket, "Distributor_R3_Master.json", df_start)

df = df_start.groupby(["RECORD_TYPE"]).agg(TOTAL_NUMBER_OF_RECORDS=pd.NamedAgg(
    column="RECORD_TYPE", aggfunc="count"), TIMESTAMP=pd.NamedAgg(column="TIMESTAMP", aggfunc="first")).reset_index()

df['RECORD_CONTROL_TYPE'] = "Control"

df.rename(columns={"RECORD_TYPE": "RECORD_CONTROL_TYPE",
          "RECORD_CONTROL_TYPE": "RECORD_TYPE"}, inplace=True)

res = putDf_To_S3(dest_bucket, "Control_Distributor_R3_Master.json", df)

'''
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# eof / last loc is ommited when this file is upload to glue script.
# '''