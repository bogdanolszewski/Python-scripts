
import pandas as pd
from fast_to_sql import fast_to_sql as fts
import pyodbc
import time

server='DP-SupplyPlanning-EU.biz.electrolux.com'
db='SupplyPlanning'
user='supply_admin'
password='f1pu0](2u9NM'
conn = pyodbc.connect('DRIVER={SQL Server};''SERVER='+server+';''Trusted_Connection=no;''Database='+db+';'
                      'UID='+user+';''PWD='+password+';')

print(f"connected to {server}, database: {db}")
cursor = conn.cursor()

sqlproc = ("""
with snp_archives as (SELECT * FROM(
	SELECT [Update],[StartDate],[AvaDate], CASE WHEN [Category]  in ('PchOrd','DEP: PReq','Deliv.','SalesOrder','SNP:VMI-SO','TLB:VMI-SO') THEN 'Distribution'
    WHEN [Category]  in ('PlOrd.','PlOrd. (F)','PP PRIO','PrdOrd (C)','PrdOrd (R)','SNP:PL-ORD') THEN 'Production' ELSE 'NULL'
	END  as [Category],
	sum(abs([Qty])) as 'Qty_sum',[Product],[Source],[Target],CONVERT(date,GETDATE()) as today
	FROM [SupplyPlanning].[dbo].[vw_DB_SNP_Orders_Archives]
	Where
	[Type]='FDC' 
	and [Product]>899999999
	and ([AvaDate]> Dateadd(dd,-7 ,[Update]) and [AvaDate]< Dateadd(dd,+15 ,[Update]) or  [StartDate]> Dateadd(dd,-7 ,[Update]) and [StartDate]< Dateadd(dd,+15 ,[Update]))
	and [Update]>=	CASE 
	WHEN DATENAME(DW,GETDATE()) = 'Monday'  THEN CONVERT (date, GETDATE()-4)
	WHEN DATENAME(DW,GETDATE()) = 'Tuesday' THEN CONVERT (date, GETDATE()-4)											
	ELSE CONVERT (date, GETDATE()-2) END 
	GROUP BY [Update],[StartDate],[AvaDate],[Category],[Source],[Product],[Target]
	HAVING [Category] <>'NULL'
)as s1
where [Product] not in ('920436227','920436239','920436245','920436260','920478969','920478971','920478975','920478977',
'920478978','920478987','920478992','920478993','920478999','920489264','920489265','920489282','920721159',
'920489290','920489295','920489300','920489302','920524573','920524586','920524587','920524591','920524594',
'920524596','920594143','920594148','920594155','920594165','920595162','920595163','920595164','920602121',
'920602136','920602140','920602141','920602144','920603152','920603153','920603154','920603155','920603156',
'920664159','920664167','920664168','920672605','920672629','920672634','920672640','920672643','920672659',
'920672664','920672665','920672669','920672670','920672672','920672675','920681097','920681099','920711305',
'920711335','920711336','920711337','920711346','920711347','920711348','920711353','920711358','920711360',
'920711363','920720169','920720172','920720173','920721145','920721153','920721154','920721155','920721156')
),
dist_prod as (
Select
	snp_archives.[Update], snp_archives.StartDate, snp_archives.AvaDate,snp_archives.Source,snp_archives.today,
	CASE WHEN [Category]='Distribution' THEN snp_archives.Qty_sum ELSE NULL END AS dist,
	CASE WHEN [Category]='Production' THEN snp_archives.Qty_sum ELSE NULL END AS prod
FROM snp_archives
),
cte2 as (
SELECT
	dist_prod.[Update], dist_prod.StartDate, dist_prod.AvaDate, dist_prod.Source, dist_prod.today,
	dist_prod.dist, dist_prod.prod,
	(datediff(day, [AvaDate], today)) as 'today minus avadate',
	(datediff(day, [StartDate], today)) as 'today minus startdate'
FROM dist_prod
),
 cte3 as (
select
	cte2.[Update], cte2.StartDate, cte2.AvaDate, cte2.Source, cte2.today,cte2.dist, cte2.prod,
	CASE WHEN cte2.[today minus avadate]>-15 THEN cte2.AvaDate
	WHEN cte2.[today minus startdate] between -15 and 0 THEN cte2.StartDate
	END AS filterdate
from cte2
),
cte4 as ( --result
select cte3.Source, cte3.[Update], sum(cte3.dist) as Total
FROM cte3
WHERE [filterdate] = Format(GETDATE(), 'yyyy-MM-dd') and [Update] = dateadd(day, -1, Format(GETDATE(), 'yyyy-MM-dd'))
and [Source] in ('DE02','HU01','HU11','IT04','IT24','IT34','IT44','PL11','PL21','PL31','PL41','RO03','UA01','PL91','IT94')
GROUP BY Source, [Update]
)
INSERT INTO [dbo].[data_disprod_frozen_history] ([Source],[Update],[Sum],[AvaData],[uploaddate])
    SELECT cte4.Source,cte4.[Update],cte4.Total,
dateadd(day, 1, cte4.[Update]) as 'AvaDate',
Format(GETDATE(), 'yyyy-MM-dd') as 'todaydate'
	FROM cte4
""")
print("executing...")
cursor.execute(sqlproc)
print("done.")
conn.commit()