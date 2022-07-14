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
    
    response = s3_client.get_object(Bucket=bucketName, Key=fileName)
    data = response['Body'].read()
    data = json.loads(data)
    df = pd.DataFrame(data['data'])
    return df

dion_df = getDF(bucket, dion)
sawyer_df = getDF(bucket, sawyer)
product_master_df = getDF(bucket, product_master)



product_master_df_copy = product_master_df[['DISTRIBUTOR_ITEM_STATUS_DESCRIPTION', 'DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_SALEABLE_PRODUCT_DESCRIPTION','DISTRIBUTOR_GALLON_CONVERSION_FACTOR', 'DISTRIBUTOR_PACK_COUNT_DESCRIPTION','DISTRIBUTOR_BASE_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_SALES_CODE_ID', 'DISTRIBUTOR_COST_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_PRICE_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_REPORTING_ITEM_ID', 'DISTRIBUTOR_INVENTORY_TYPE', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE', 'DISTRIBUTOR_SHIPPABLE_PRODUCT_GROUP']]

lookup1 = pd.merge(dion_df, product_master_df_copy, on='DISTRIBUTOR_ITEM_ID')

lookup1_copy = lookup1

lookup1 = lookup1.iloc[:, 0:17]

product_master_df_copy['DISTRIBUTOR_PACK_COUNT_DESCRIPTION'] = product_master_df_copy.DISTRIBUTOR_PACK_COUNT_DESCRIPTION.astype(str).astype(float)


lookup1['newCol'] = lookup1.DISTRIBUTOR_COMPONENT1_QUANTITY * product_master_df_copy.DISTRIBUTOR_PACK_COUNT_DESCRIPTION

lookup1['newCol2'] = np.where(lookup1['DISTRIBUTOR_COMPONENT1_QUANTITY'] == lookup1_copy['DISTRIBUTOR_GALLON_CONVERSION_FACTOR'], 'Match', 'Do Not Match')

depivoted_df = pd.melt(lookup1, id_vars=['DISTRIBUTOR_R3_TYPE', 'DISTRIBUTOR_ITEM_ID', 'RECORD_TYPE', 'DISTRIBUTOR_COMPONENT1_QUANTITY', 'DISTRIBUTOR_WAREHOUSE_ID', 'DISTRIBUTOR_COMPONENT2_QUANTITY', 'DISTRIBUTOR_COMPONENT3_QUANTITY', 'DISTRIBUTOR_COMPONENT4_QUANTITY','DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT','DISTRIBUTOR_COMPONENT2_UNIT_OF_MEASUREMENT','DISTRIBUTOR_COMPONENT3_UNIT_OF_MEASUREMENT','DISTRIBUTOR_COMPONENT4_UNIT_OF_MEASUREMENT'], 
                       value_vars=['DISTRIBUTOR_COMPONENT1_ITEM_ID','DISTRIBUTOR_COMPONENT2_ITEM_ID','DISTRIBUTOR_COMPONENT3_ITEM_ID','DISTRIBUTOR_COMPONENT4_ITEM_ID'],
                       var_name='REFERENCE', value_name='COMPONENT_COMPUTED'
                )

depivoted_df = depivoted_df[depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT1_ITEM_ID']

depivoted_df['UNIT_OF_MEASUREMENT(COMPUTED)'] = np.where(depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT1_ITEM_ID', depivoted_df.DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT, 
                                                np.where(depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT2_ITEM_ID', depivoted_df.DISTRIBUTOR_COMPONENT2_UNIT_OF_MEASUREMENT,
                                                np.where(depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT3_ITEM_ID', depivoted_df.DISTRIBUTOR_COMPONENT3_UNIT_OF_MEASUREMENT,
                                                np.where(depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT4_ITEM_ID', depivoted_df.DISTRIBUTOR_COMPONENT4_UNIT_OF_MEASUREMENT,''
                                                        ))))

depivoted_df['COMPONENT_QUANTITY(COMPUTED)'] = np.where(depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT1_ITEM_ID', depivoted_df.DISTRIBUTOR_COMPONENT1_QUANTITY, 
                                                np.where(depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT2_ITEM_ID', depivoted_df.DISTRIBUTOR_COMPONENT2_QUANTITY,
                                                np.where(depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT3_ITEM_ID', depivoted_df.DISTRIBUTOR_COMPONENT3_QUANTITY,
                                                np.where(depivoted_df.REFERENCE == 'DISTRIBUTOR_COMPONENT4_ITEM_ID', depivoted_df.DISTRIBUTOR_COMPONENT4_QUANTITY,''
                                                        ))))


depivoted_df = depivoted_df.drop(columns=['DISTRIBUTOR_COMPONENT1_UNIT_OF_MEASUREMENT','DISTRIBUTOR_COMPONENT2_UNIT_OF_MEASUREMENT','DISTRIBUTOR_COMPONENT3_UNIT_OF_MEASUREMENT','DISTRIBUTOR_COMPONENT4_UNIT_OF_MEASUREMENT',
'DISTRIBUTOR_COMPONENT1_QUANTITY','DISTRIBUTOR_COMPONENT2_QUANTITY','DISTRIBUTOR_COMPONENT3_QUANTITY','DISTRIBUTOR_COMPONENT4_QUANTITY' ])


product_master_df_copy2 = product_master_df[['DISTRIBUTOR_ITEM_STATUS_DESCRIPTION','DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_ITEM_DESCRIPTION', 'DISTRIBUTOR_GALLON_CONVERSION_FACTOR', 'MANUFACTURER_NAME'
                                              , 'DISTRIBUTOR_SALES_CODE_ID', 'DISTRIBUTOR_COST_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_PRICE_UNIT_OF_MEASUREMENT', #'WARNING: BLANK DISTRIBUTOR PRICE UNIT OF MEASUREMENT',
                                              #'WARNING: BLANK DISTRIBUTOR COST UNIT OF MEASUREMENT', 'WARNING: STANDARD COST IS GREATER THAN SELL PRICE', # not in file
                                               'DISTRIBUTOR_REPORTING_ITEM_ID', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE', # (2) diff versions exist at many cols
                                              'DISTRIBUTOR_PACK_DESCRIPTION', 'DISTRIBUTOR_UNITS_PER_LAYER' #'Units per layer' not in json file.
                                            ]]

lookup2 = pd.merge(depivoted_df, product_master_df_copy2, on='DISTRIBUTOR_ITEM_ID') # some cols are missing 

output1 = lookup2.drop(columns=['DISTRIBUTOR_GALLON_CONVERSION_FACTOR', 'MANUFACTURER_NAME', 'DISTRIBUTOR_COST_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_PRICE_UNIT_OF_MEASUREMENT', ])

output2 = output1[output1['DISTRIBUTOR_SALES_CODE_ID'] != '4']

product_master_df_copy3 = product_master_df[['DISTRIBUTOR_ITEM_ID', 'DISTRIBUTOR_ITEM_DESCRIPTION', 'MANUFACTURER_NAME','DISTRIBUTOR_COST_UNIT_OF_MEASUREMENT',
                                            'DISTRIBUTOR_PRICE_UNIT_OF_MEASUREMENT', 'DISTRIBUTOR_REPORTING_ITEM_ID', 'DISTRIBUTOR_INVENTORY_TYPE', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE' #(3)', 'DISTRIBUTOR_ITEM_INVENTORY_STATUS_CODE (1)'
                                            , 'DISTRIBUTOR_UNITS_PER_LAYER' #'Units per layer' not in json file.
                                            ]]


lookup3 = pd.merge(output1, product_master_df_copy3, left_on='COMPONENT_COMPUTED' ,right_on='DISTRIBUTOR_ITEM_ID')

lookup3 = lookup3.drop(columns=['DISTRIBUTOR_WAREHOUSE_ID', 'DISTRIBUTOR_INVENTORY_TYPE'])

lookup3_copy = lookup3.groupby(['COMPONENT_COMPUTED'])

print(lookup3_copy.count().nunique) # wrong, requirement is diff