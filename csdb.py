import vaex as vx
import pandas as pd
from sqlalchemy import create_engine
import os
import mysql.connector
from mysql.connector import errorcode
import datetime as datetime
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re

#function to parser the dates to datetime object
def parser(x):
    return pd.to_datetime(x, dayfirst=True)

# Extract the data from the csv file using vaex
df = vx.read_csv(r'C:/Users/mpharma/Downloads/vdl.csv',
                dtype={'Material ID':str},
                 parse_dates=['Created On'], date_parser=lambda x: parser(x))

# Get the required columns
vdl = df['Material ID', 'Unnamed: 1', 'Product Category', 'Created On', 'Brand / Proprietary Name', 'Form', 'Generic Name','Manufacturer', 'OTC/POM', 'Strength', 'Sub Category', 'Tier']

# Get the required Country
gh_vdl = vdl[vdl['Material ID'].str.contains('GH')]


# Renamming columnns
gh_vdl.rename(name='Unnamed: 1', new_name='Product_Name')
gh_vdl.rename(name='Brand / Proprietary Name', new_name='Brand_Name')

for i in gh_vdl.column_names:
    name = i.replace(" ", "_").replace("/", "_")
    gh_vdl.rename(i, name)


# Including pack sizes to dataset
def size_parser(x):
    find = re.findall("[xX][0-9]*$", x)
    try:
        size = find[0].replace("x", "").replace("X", "")
    except IndexError:
        size = 0
    return size

gh_vdl['Pack_Size'] = gh_vdl['Product_Name'].apply(lambda x: size_parser(x))


# Convert vx dataframe to pandas for database upload
pd_gh= gh_vdl.to_pandas_df()

# Configuration for database
Cfg = {'user':'root',
        'password':'password',
        'host': 'localhost',
        'database':'mpharma',
        'raise_on_warnings':True, 
        'auth_plugin':'mysql_native_password'}

# Connecting to MySQL DB using the mysqlconnector
cnx = mysql.connector.connect(**Cfg)

cursor = cnx.cursor()
# Creating table in MySQl is non-existent

create = "CREATE TABLE vdl(Material_ID VARCHAR(50) PRIMARY KEY NOT NULL, \
        Product_Name VARCHAR(225) NOT NULL, Product_Category VARCHAR(225),\
        Created_On DATE NOT NULL, Brand_Name VARCHAR(225), Form VARCHAR(225), \
        Generic_Name VARCHAR(225), Manufacturer VARCHAR(225), OTC_POM VARCHAR(225), \
        Strength VARCHAR(225), Sub_Category VARCHAR(225), \
        Tier VARCHAR(225), Pack_Size INT(5))"


try:
    print("Creating table {}: ".format('vdl'), end='')
    cursor.execute(create)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
        print("already exists.")
    else:
        print(err.msg)
else:
    print("OK")
cursor.execute('DESC vdl')    
cursor.fetchall()

cursor.execute("SELECT Product_Name, Created_On FROM vdl ORDER BY YEAR(Created_On) DESC LIMIT 1")
print(cursor.fetchone())
curr_query = cursor.statement
cursor.close()
cnx.close()

# Move to sqlalchemy for read and write functions
engine_url = "mysql+mysqlconnector://" + Cfg['user'] + ":" + \
     Cfg['password'] + "@" + Cfg['host'] + "/" + Cfg['database']
# Connecting to mysql db using sqlalchemy
engine = create_engine(engine_url)

curr_data_query = "SELECT * FROM vdl"
# read the query for the recently made product
current_date = pd.read_sql_query(curr_query, con=engine, dtype={'Created_On':np.datetime64})['Created_On'][0]

# Compare the current date to the df to only insert newly made products
insert_data = pd_gh[pd_gh['Created_On'] > current_date]
if not insert_data.empty:
# Went with this approach because its blazingly fast compared with normal insert queries
    insert_data.to_sql('vdl', con = engine, if_exists = 'append', index = False)
    upload_data = pd.read_sql_query(curr_data_query, con=engine, dtype={'Created_On':np.datetime64})
else:
    upload_data = pd.read_sql_query(curr_data_query, con=engine, dtype={'Created_On':np.datetime64})

upload_data['Created_On'] = upload_data['Created_On'].dt.strftime('%Y-%m-%d')


# Google Sheet Credentials for updates
os.chdir(r'C:\Users\mpharma\Projects\mpharma\mPharma_Order_Processing\Credentials for Google API')

scope = ["https://spreadsheets.google.com/feeds", 
         'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", 
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("cred.json", scope)
client = gspread.authorize(creds)

main_sheet = client.open("CS MASTER SHEET")
vdl_sheet = main_sheet.worksheet('NEW_VDL')
vdl_sheet.batch_clear(["I2:Z"])
vdl_sheet.update("I2:Z",[upload_data.columns.values.tolist()]+ upload_data.values.tolist())












