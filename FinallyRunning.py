#imports
from __future__ import division
from pyspark.sql.functions import collect_list, collect_set, count, col, size, sum, udf, array, explode
from pyspark.sql.types import StringType, ArrayType
import string
import collections
from itertools import chain
import re
import timeit                       t_sda01.c_cdh_household AS b
import sys
from pyspark import SparkConf, SparkContext, HiveContext b.rowid_object
print(" ergsklghashdhasjf sdhkghgshk", sys.argv[1])
argu=sys.argv[1]r>  to exit Vim                                                                                                                                                         11,1          Top
conf = SparkConf().setAppName('Mini Bridge Table').set("spark.executor.memory", "64g").set("spark.driver.memory", "32g")
sc = SparkContext(conf = conf)
#sc._conf.set('spark.driver.maxResultsSize','0')                                                                                                                                        1,1           Top

#sc.setLogLevel("ERROR")
hc = HiveContext(sc)


df4=hc.sql("""SELECT a.create_date as Customer_create_date,
                            a.rowid_object AS Party_id,
                            b.rowid_cdh_household

                     FROM   t_sda01.c_cdh_party AS a LEFT OUTER JOIN
                            t_sda01.c_cdh_household_prty_rel AS b

                     ON  a.rowid_object=b.rowid_cdh_party
                        """)
#df4.persist(StorageLevel.MEMORY_AND_DISK)
        #df.cache()

df4.registerTempTable("tmp_bridge1")


df5=hc.sql("""SELECT a.Customer_create_date,
                     a.Party_id,
                     b.rowid_object AS Household_id


              FROM   tmp_bridge1 AS a LEFT OUTER JOIN
                     t_sda01.c_cdh_household AS b

              ON  a.rowid_cdh_household = b.rowid_object
            """)

df5.registerTempTable("tmp_bridge2")


df6=hc.sql("""SELECT a.Customer_create_date,
                     a.Party_id as People_id,
                     a.Household_id,
                     b.phone_contact_id

             FROM   tmp_bridge2 AS a LEFT OUTER JOIN
                    t_sda01.c_cdh_prty_phone_contact AS b

             ON  a.Party_id = b.party_id
           """)

df6.registerTempTable("tmp_bridge3")


df7=hc.sql("""SELECT a.Customer_create_date,
                     a.People_id,
                     a.Household_id,
                     b.phone_area_cd, b.phone_num

              FROM   tmp_bridge3 AS a LEFT OUTER JOIN
                     t_sda01.c_cdh_phone_contact AS b

             ON  a.phone_contact_id = b.rowid_object
            """)

df7.registerTempTable("tmp_bridge4")
#done till phone----------------------------------------------------------------

df8=hc.sql("""SELECT a.Customer_create_date,
                     a.People_id,
                     a.Household_id,
                     a.phone_area_cd,
                     a.phone_num,
                     b.email_id

             FROM   tmp_bridge4 AS a LEFT OUTER JOIN
                    t_sda01.c_cdh_prty_email AS b

             ON  a.People_id = b.party_id
            """)
df8.registerTempTable("tmp_bridge5")

df9=hc.sql("""SELECT a.Customer_create_date,
                            a.People_id,
                            a.Household_id,
                            a.phone_area_cd,
                            a.phone_num,
                            b.email_addr_nm as email_address

                     FROM   tmp_bridge5 AS a LEFT OUTER JOIN
                            t_sda01.c_cdh_email AS b

                     ON  a.email_id = b.rowid_object
                        """)


df9.registerTempTable("tmp_bridge6")
#done till email----------------------------------------------------------------

my_rdd=df9.rdd


#This function converts a full date(eg "2004-09-03 17:25:11") to a month date(eg "2004-09")
def month_extractor(full_date):
    month_date=full_date[0:7]
    return month_date


#monthName = input('Enter the month(YYYY-MM):    ')
matchObj1 = re.match( r'[0-9]{4}[-][0][1-9]|[0-9]{4}[-][1][0-2]', argu)
#print("i'm out", argu)
#while(1):
if matchObj1:
        #print("hi")
       # monthName1=monthName[0:7]
       # print (monthName1)
        monthly_rdd = my_rdd.filter(lambda x: month_extractor(x[0]) == argu)
        s1=monthly_rdd.toDF()
        s1.show()

else:
        print('Invalid input, please try again in (YYYY-MM)')
       # input_feed = input()
        #if input_feed=='q':
print("\n Do you want to save this snapshot? (y/n)  :")
#saveInput = input()
matchObj2 = re.match( r'[y]|[n]', monthName)
if saveInput[0]=="y":
    s1.saveAsTable("MonthlySnapshotBridge")
elif saveInput[0]=="n":
    print("GoodBye")
else:
    print("Invalid input")
