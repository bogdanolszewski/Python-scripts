import pandas as pd
import numpy as np
import os
from fast_to_sql import fast_to_sql as fts
import pyodbc
import time
from datetime import timedelta
from datetime import datetime
from datetime import date
import glob

sapfiles = r"\\plws0125\SUPPLY_PLANNING\!tools\Dis&prod\Sent done SAP.XLSX"
otdfiles = r"\\plws0125\SUPPLY_PLANNING\!tools\Dis&prod\Sent done OTD"
factoryperf = r"\\plws0125\SUPPLY_PLANNING\!tools\Dis&prod\FactoryPerformances.xlsx"

df1 = []
df1 = pd.DataFrame(df1)
os.chdir(otdfiles)
for file in glob.glob(otdfiles+"\*"):
    df = pd.read_excel(file)
    df1 = df1.append(df)
df1['QTY SHIP.'] = df1['QTY SHIP.'].fillna(0)
df1 = df1.rename(columns={"QTY SHIP.": "Shipping", "DESP. ADV. DATE":"Date ADJ"})
df1= df1[df1['ITEM ID']<1000000000]
df1['Plant'] = df1['SELLER'].map({131:"UA01", 330:"IT04",333:"IT24",
                                334:"IT44",335:"IT34",395:"DE02",
                                275:"PL91","274/ITB":"IT94"})
df1 = df1.drop(["SHIP FROM", "DELIVERY TO", "ITEM ID", "GIT", "GIT STATUS",
         "BUYER","SELLER","DESP. ADV. N.","ASSET ID", "DOC. ID", "NOTE"], axis=1)
print(df1)

df2 = pd.read_excel(sapfiles)
df2['Date ADJ'] = np.where(df2['Posting Date'].dt.dayofweek==7, df2['Posting Date']-timedelta(days=1), df2['Posting Date'])
df2['Qty'] = abs(df2["Qty in Un. of Entry"])
df2 = df2.drop(["Storage Location", "Material", "Material Description", "Time of Entry", "User name","Qty in Un. of Entry",
            "Special Stock", "Material Document", "Material Doc.Item", "Reference", "Purchase Order",
            "Vendor", "Customer", "Text", "Unit of Entry", "Entry Date", "Amount in LC", "Posting Date",], axis=1)
df2 = df2 = df2.drop(["Movement Type"], axis=1)
df2 = df2.rename(columns={"Qty": "Shipping"})
df1 = df1.iloc[:, [2,1,0]] #reordering the columns
df3 = pd.concat([df2, df1], ignore_index=True)

df3['Date ADJ'] = pd.to_datetime(df3['Date ADJ'], format='%Y-%m-%d', errors='coerce')#10-05-22: had to add arguments:
#errors='coerce', format='%Y-%m-%d'
td = date.today()
td_minus_4 = (td-timedelta(days=4)).strftime('%Y-%m-%d')

df3['test'] = np.where(df3['Date ADJ']>=td_minus_4, "OK", "NOK")
df3['test2'] = np.where(df3['Date ADJ']==td, "NOK", "OK")

df3 = df3[(df3['test']=="OK") & (df3['test2']=="OK")]
df3 = pd.DataFrame(df3.groupby(["Plant","Date ADJ"]).agg({"Shipping": "sum"}).reset_index())
#---
df4 = pd.read_excel(factoryperf)
df4['Date'] = pd.to_datetime(df4['Date'])
df4 = df4.drop(["Factory", "GPH Sub Group", "Old Sub Group", "Item GR", "Planned", "KPI03", "Delta", "DSA Code",
          "DSA Extra Reason", "ANC Component Code", "Factory Supplier", "Sales Company", "Equipment Code",
          "Action Description", "Action Responsible", "Action Status"], axis=1)
df4['Plant'] = df4['Node'].map({"PLT":"PL31","PLV":"PL21","PLY":"PL41","PLS":"PL11","ROB":"RO03",
                                "ZS":"IT24","ZP":"IT04","ZM":"IT34","ZO":"IT44","UKE":"UA01","HUC":"HU01",
                                "HUY":"HU11","DGT":"DE02","DMU":"DE02","PLB":"PL91","ITB":"IT94"})
df4 = df4.dropna(axis=0)
df4 = pd.DataFrame(df4.groupby(["Plant","Date"]).agg({"Achieved": "sum"}).reset_index())

df3 = df3[df3['Date ADJ']==(date.today()-timedelta(days=1)).strftime('%Y-%m-%d')]
df4 = df4[df4['Date']==(date.today()-timedelta(days=1)).strftime('%Y-%m-%d')]

df5 = pd.merge(df3, df4, on = "Plant", how = "left")
df5 = df5.drop(columns=["Date"])
df5['Achieved'] = df5['Achieved'].fillna(0)
df5['Shipping'] = df5['Shipping'].astype(np.int64)
df5['Achieved'] = df5['Achieved'].astype(np.int64)
df5['weekday'] = (df5['Date ADJ'].dt.dayofweek)+1 #+1 because week starts from 0(monday)
df5['weekday'] = df5['weekday'].astype(np.int64)
df5 = df5.iloc[:, [0,1,2,3,4]]
print(df5.head(5))


server='DP-SupplyPlanning-EU.biz.electrolux.com'
db='SupplyPlanning'
user='supply_admin'
password='f1pu0](2u9NM'
conn = pyodbc.connect('DRIVER={SQL Server};''SERVER='+server+';''Trusted_Connection=no;''Database='+db+';'
                      'UID='+user+';''PWD='+password+';')
cursor = conn.cursor()
#cursor.execute("SELECT MAX([upload_date]) FROM [SupplyPlanning].[dbo].[data_disprod_shippings_history]")
today = (date.today()).strftime('%Y-%m-%d')
sqlstr = """
INSERT INTO [dbo].[data_disprod_shippings_history](
[Factory],[Date],[Shippings],[Production],[Day_of_week],[upload_date]
)
     VALUES(
"""
for i in range(0, len(df5)):
    dfvals = "'"+str(df5.iloc[i,0])+"','"+str(df5.iloc[i,1].strftime('%Y-%m-%d'))+"','"+str(df5.iloc[i,2])+"','"+str(df5.iloc[i,3])+"','"+str(df5.iloc[i,4])+"', '"+today+"'"
    sqlstr = sqlstr+dfvals+")"
    print(sqlstr)
    cursor.execute(sqlstr)
    conn.commit()
    sqlstr = """
    INSERT INTO [dbo].[data_disprod_shippings_history](
    [Factory],[Date],[Shippings],[Production],[Day_of_week],[upload_date]
    )
         VALUES("""
print("rows imported succesfully.")