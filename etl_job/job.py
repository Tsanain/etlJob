import boto3
import json
import pandas as pd
import numpy as np

import warnings
warnings.filterwarnings("ignore")

bucket, dion, sawyer, product_master = "tsan-bucket-trial", "Dion-DISTRIBUTOR_R3_MASTER-2022-07-04_19_00_00.json", "Sawyer-DISTRIBUTOR_R3_MASTER-2022-07-04_19_00_00.json", "Dion-PRODUCT_MASTER_DISTRIBUTOR_OUTPUT-yyyy-mm-dd_19_00_00.json"

s3_obj = boto3.resource('s3')
s3_client = boto3.client('s3')


def getDF(bucketName, fileName):

    res = s3_client.get_object(Bucket=bucketName, Key=fileName)
    data = res['Body'].read()
    data = json.loads(data)
    df = pd.DataFrame(data['data'])
    return df


def writeJsonTos3(bucket, filename, df):
    output = {}
    output['data'] = df.to_dict(orient='records')
    object = s3_obj.Object(bucket, filename)
    result = object.put(Body=json.dumps(output))
    return result


def printCol(df):
    i = 0
    for col in df.columns:
        print(col)
        i += 1
    print("count:", i)


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


dion_df = getDF(bucket, dion)
sawyer_df = getDF(bucket, sawyer)
product_master_df = getDF(bucket, product_master)


product_master_df_copy = product_master_df[['DISTRIBUTOR_ITEM_STATUS_DESCRIPTION', 'DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_SALEABLE_PRODUCT_DESCRIPTION', 'DISTRIBUTOR_GALLON_CONVERSION_FACTOR', 'DISTRIBUTOR_PACK_COUNT_DESCRIPTION', 'DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT',
                                            'DISTRIBUTOR_SALES_CODE_ID', 'DISTRIBUTOR_COST_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_PRICE_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_REPORTING_ITEM_ID', 'DISTRIBUTOR_INVENTORY_TYPE', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE', 'DISTRIBUTOR_SHIPPABLE_PRODUCT_GROUP']]

lookup1 = pd.merge(dion_df, product_master_df_copy, on='DISTRIBUTOR_ITEM_ID')

lookup1_copy = lookup1

lookup1 = lookup1.iloc[:, 0:17]

product_master_df_copy['DISTRIBUTOR_PACK_COUNT_DESCRIPTION'] = product_master_df_copy.DISTRIBUTOR_PACK_COUNT_DESCRIPTION.astype(
    str).astype(float)


# not used any where

lookup1['newCol'] = lookup1.DISTRIBUTOR_COMPONENT1_QUANTITY * \
    product_master_df_copy.DISTRIBUTOR_PACK_COUNT_DESCRIPTION

lookup1['newCol2'] = np.where(lookup1['DISTRIBUTOR_COMPONENT1_QUANTITY'] ==
                              lookup1_copy['DISTRIBUTOR_GALLON_CONVERSION_FACTOR'], 'Match', 'Do Not Match')


# rows to be removed and col to be renamed, steps not opening.
res = writeJsonTos3(
    bucket, 'R3 Exceptions: Component1 Item Quantity and Conversion Factor Do Not Match.json', lookup1)

depivoted_df = pd.melt(lookup1, id_vars=['DISTRIBUTOR_R3_TYPE', 'DISTRIBUTOR_ITEM_ID', 'RECORD_TYPE', 'DISTRIBUTOR_COMPONENT1_QUANTITY', 'DISTRIBUTOR_WAREHOUSE_ID', 'DISTRIBUTOR_COMPONENT2_QUANTITY', 'DISTRIBUTOR_COMPONENT3_QUANTITY', 'DISTRIBUTOR_COMPONENT4_QUANTITY', 'DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPONENT2_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPONENT3_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPONENT4_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_COMPANY_ID'],
                       value_vars=['DISTRIBUTOR_COMPONENT1_ITEM_ID', 'DISTRIBUTOR_COMPONENT2_ITEM_ID',
                                   'DISTRIBUTOR_COMPONENT3_ITEM_ID', 'DISTRIBUTOR_COMPONENT4_ITEM_ID'],
                       var_name='REFERENCE', value_name='COMPONENT_COMPUTED'
                       )

# mistake
#depivoted_df = depivoted_df[depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT1_ITEM_ID']

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


product_master_df_copy2 = product_master_df[['DISTRIBUTOR_ITEM_STATUS_DESCRIPTION', 'DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_ITEM_DESCRIPTION', 'DISTRIBUTOR_GALLON_CONVERSION_FACTOR', 'MANUFACTURER_NAME', 'DISTRIBUTOR_SALES_CODE_ID', 'DISTRIBUTOR_COST_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_PRICE_UNIT_OF_MEASUREMENT',  # 'WARNING: BLANK DISTRIBUTOR PRICE UNIT OF MEASUREMENT',
                                             # 'WARNING: BLANK DISTRIBUTOR COST UNIT OF MEASUREMENT', 'WARNING: STANDARD COST IS GREATER THAN SELL PRICE', # not in file
                                             # (2) diff versions exist at many cols
                                             'DISTRIBUTOR_REPORTING_ITEM_ID', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE',
                                             # 'Units per layer' not in json file.
                                             'DISTRIBUTOR_PACK_DESCRIPTION', 'DISTRIBUTOR_UNITS_PER_LAYER'
                                             ]]

lookup2 = pd.merge(depivoted_df, product_master_df_copy2,
                   on='DISTRIBUTOR_ITEM_ID')  # some cols are missing

output1 = lookup2.drop(columns=['DISTRIBUTOR_GALLON_CONVERSION_FACTOR', 'MANUFACTURER_NAME',
                       'DISTRIBUTOR_COST_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_PRICE_UNIT_OF_MEASUREMENT', ])

output1.rename(columns={'Columns': 'REFERENCE', 'COMPONENT_COMPUTED': 'DISTRIBUTOR_COMPONENT1_ITEM_ID', 'UNIT_OF_MEASUREMENT(COMPUTED)':
               'DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT', 'COMPONENT_QUANTITY(COMPUTED)': 'DISTRIBUTOR_COMPONENT1_QUANTITY'}, inplace=True)

res = writeJsonTos3(
    bucket, 'Output: R3 Table Before Sales Code Removal Step.json', output1)


# print(res)


output2 = output1[output1['DISTRIBUTOR_SALES_CODE_ID'] != '4']

output2.rename(columns={'Columns': 'REFERENCE', 'COMPONENT_COMPUTED': 'DISTRIBUTOR_COMPONENT1_ITEM_ID', 'UNIT_OF_MEASUREMENT(COMPUTED)':
               'DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT', 'COMPONENT_QUANTITY(COMPUTED)': 'DISTRIBUTOR_COMPONENT1_QUANTITY'}, inplace=True)

res = writeJsonTos3(bucket, 'Output: Micro - De-Pivot R3 Table.json', output2)

# print(res)


product_master_df_copy3 = product_master_df[['DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_ITEM_DESCRIPTION', 'MANUFACTURER_NAME', 'DISTRIBUTOR_COST_UNIT_OF_MEASUREMENT',
                                            # (3)', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE (1)'
                                             # 'Units per layer' not in json file.
                                             'DISTRIBUTOR_PRICE_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_REPORTING_ITEM_ID', 'DISTRIBUTOR_INVENTORY_TYPE', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE', 'DISTRIBUTOR_UNITS_PER_LAYER'
                                             ]]


lookup3 = pd.merge(output1, product_master_df_copy3,
                   left_on='DISTRIBUTOR_COMPONENT1_ITEM_ID', right_on='DISTRIBUTOR_ITEM_ID')

lookup3 = lookup3.drop(
    columns=['DISTRIBUTOR_WAREHOUSE_ID', 'DISTRIBUTOR_INVENTORY_TYPE'])

#lookup3_copy = lookup3.groupby(['DISTRIBUTOR_COMPONENT1_ITEM_ID'])

# print(lookup3_copy.count().nunique) # wrong, requirement is diff

printCol(lookup3)

df = lookup3.groupby(['RECORD_TYPE', 'DISTRIBUTOR_ITEM_ID'])

df = df.count().reset_index()

print(df)

###############################################################################################################################################################################################################################################################################################################################################################################

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
                   on='DISTRIBUTOR_ITEM_ID')


lookup4.drop(columns=['DISTRIBUTOR_SHIPPABLE_PRODUCT_GROUP'], inplace=True)


res = writeJsonTos3(
    bucket, 'Output: SP R3 Table Before Sales Code Removal Step.json', lookup4)

lookup4_copy = lookup4[lookup4['DISTRIBUTOR_SALES_CODE_ID'] != '4']

# less no. of rows as product master is diff, and has less rows.
res = writeJsonTos3(
    bucket, 'Output: Micro - De-Pivot SP R3 Table.json', lookup4_copy)

product_master_df_copy5 = product_master_df[['DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_ITEM_DESCRIPTION', 'MANUFACTURER_NAME',
                                             'DISTRIBUTOR_REPORTING_ITEM_ID', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE', 'DISTRIBUTOR_UNITS_PER_LAYER']]

product_master_df_copy5.rename(columns={'DISTRIBUTOR_ITEM_DESCRIPTION': 'DISTRIBUTOR_ITEM_DESCRIPTION (DISTRIBUTOR_ITEM_ID)',
                               'MANUFACTURER_NAME': 'MANUFACTURER_NAME (COMPONENT1)', 'DISTRIBUTOR_ITEM_ID': 'DISTRIBUTOR_ITEM_ID(1)'}, inplace=True)

# not sure if merge is on lookup4 or not.

lookup5 = pd.merge(lookup4, product_master_df_copy5,
                   left_on='DISTRIBUTOR_COMPONENT1_ITEM_ID', right_on='DISTRIBUTOR_ITEM_ID(1)')

lookup5.drop(columns=['DISTRIBUTOR_WAREHOUSE_ID'])

# wrong

df = lookup5.groupby(['RECORD_TYPE', 'DISTRIBUTOR_ITEM_ID'])

df = df.count().reset_index()

#print(df[['RECORD_TYPE', 'DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_COMPONENT1_ITEM_ID']])

###############################################################################################################################################################################################################################################################################################################################################################################

df_start = getDF(
    bucket, 'Output: R3 Table Before Sales Code Removal Step.json')

df_import = getDF(
    bucket, 'Output: SP R3 Table Before Sales Code Removal Step.json')