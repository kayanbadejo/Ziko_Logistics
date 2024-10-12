#  Import the Necessary Libraries 

import pandas as pd
import os
import io
from azure.storage.blob import BlobServiceClient, BlobClient
from dotenv import load_dotenv

# Extracting the Data From Source 

Ziko_DF = pd.read_csv(r'ziko_logistics_data.csv')


# Data Cleaning and Transformation

# Create  a copy of the Data Frame
ZikoDF = Ziko_DF.copy()

# Fill the Missing Values
ZikoDF.fillna({ 
               'Unit_Price' : ZikoDF['Unit_Price'].mean(),
               'Total_Cost' : ZikoDF['Total_Cost'].mean(),
               'Discount_Rate' : 0.0,
               'Return_Reason' : 'Unknown'
               }, inplace = True)


ZikoDF['Date'] = pd.to_datetime(ZikoDF['Date'])

# Create the Customers Table

Customers = ZikoDF[['Customer_ID' ,'Customer_Name', 'Customer_Phone', 'Customer_Email', 'Customer_Address']].copy().drop_duplicates().reset_index(drop=True)


# Create the Products Table

Products = ZikoDF[['Product_ID', 'Product_List_Title', 'Unit_Price']].copy().drop_duplicates().reset_index(drop=True)

TransactionsFACTS = ZikoDF.merge(Customers, on = ['Customer_ID' ,'Customer_Name', 'Customer_Phone', 'Customer_Email', 'Customer_Address'], how = 'left') \
                          .merge (Products, on = ['Product_ID', 'Product_List_Title', 'Unit_Price'], how = 'left') \
                          [['Transaction_ID', 'Date', 'Customer_ID', 'Product_ID', 'Quantity', 'Total_Cost', 'Discount_Rate', 'Sales_Channel','Order_Priority', \
                            'Warehouse_Code','Ship_Mode', 'Delivery_Status','Customer_Satisfaction', 'Item_Returned', 'Return_Reason','Payment_Type', 'Taxable', \
                            'Region', 'Country']]
                          


TransactionsFACTS['Date'] = TransactionsFACTS['Date'].astype('datetime64[us]')


# Creating CSV Files for Temp Loading
Customers.to_csv(r'Datasets/Customers.csv', index =False)
Products.to_csv(r'Datasets/Products.csv', index =False)
TransactionsFACTS.to_csv(r'Datasets/TransactionsFACTS.csv', index =False)


print('Files have been loaded temporarily in the Local directory')



# Data Loading to Azure
# Setup Azure Blob Connection 

# Call  the ENV file
load_dotenv('Ziklogistic.env', override=True)

connect_str = os.getenv('Connection_Str') 

blob_service_client = BlobServiceClient.from_connection_string(connect_str)

CNTName = os.getenv('Container_Name')

CNTClient = blob_service_client.get_container_client(CNTName)

# Creating a function for Data Loading into Azure

def Upld (df, CNT_Client, blobNAME): 
    buffer =  io.BytesIO()
    df.to_parquet(buffer, index = False)
    buffer.seek(0)
    blob_client = CNT_Client.get_blob_client(blobNAME) 
    blob_client.upload_blob(buffer, blob_type = 'BlockBlob', overwrite = True)
    print(f'{blobNAME} uploaded to Blob Storage Successfully')
    
    

#Upload data as  Parquet to Blob Storage
Upld(Customers, CNTClient, 'rawdata/Customers.parquet') 
Upld(Products, CNTClient, 'rawdata/Products.parquet') 
Upld(TransactionsFACTS, CNTClient, 'rawdata/TransactionsFACTS.parquet') 