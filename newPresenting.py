#CODE: Baby bridge Table
#This Pyspark application creates a BabyBridge Table
#Program Input: Date in the format (YYYY-MM) given as a CL Argument with spark-submit
#Program Output: A hive table called snapshot_table_<YYYY>_<MM>
#Author: Sourav Bhowmik, Data Lab Team
#Last Modified: Oct 11, 2016

#Imports
from __future__ import division
from pyspark.sql.functions import collect_list, collect_set, count, col, size, sum, udf, array, explode
from pyspark.sql.types import StringType, ArrayType
import string
import collections
from itertools import chain
import re
import timeit
import sys
from pyspark import SparkConf, SparkContext, HiveContext

#Storing the CL argument in the variable DateArg.
DateArg=sys.argv[1]

#Validating DateArg using Regex.
matchObj1 = re.match( r'[0-9]{4}[-][0][1-9]|[0-9]{4}[-][1][0-2]', DateArg)
if matchObj1:
        print("Hi, correct argument")

else:
        print('Invalid input, please try again in (YYYY-MM) format. \nTerminating Program..........')
        #Terminate program in case DateArg value is invalid.
        sys.exit(0)

#Initializing SparkContext and HiveContext.
conf = SparkConf().setAppName('Mini Bridge Table').set("spark.executor.memory", "64g").set("spark.driver.memory", "32g")
sc = SparkContext(conf = conf)
hc = HiveContext(sc)

#Joining tables c_cdh_party and c_cdh_household_prty_rel.
df_party_hld_prty_rel=hc.sql("""SELECT a.create_date as Customer_create_date,
                     a.rowid_object AS Party_id,
                     b.rowid_cdh_household

              FROM   t_sda01.c_cdh_party AS a LEFT OUTER JOIN
                     t_sda01.c_cdh_household_prty_rel AS b

              ON     a.rowid_object=b.rowid_cdh_party
          """)

#Registering as a temporary table for reuse.
df_party_hld_prty_rel.registerTempTable("tmp_bridge1")

#Joining tables tmp_bridge1 and c_cdh_household
df_tb1_household=hc.sql("""SELECT a.Customer_create_date,
                     a.Party_id,
                     b.rowid_object AS Household_id

              FROM   tmp_bridge1 AS a LEFT OUTER JOIN
                     t_sda01.c_cdh_household AS b

              ON     a.rowid_cdh_household = b.rowid_object
          """)

df_tb1_household.registerTempTable("tmp_bridge2")

#Joining tables tmp_bridge2 and c_cdh_prty_phone_contact
df_tb2_prty_ph=hc.sql("""SELECT a.Customer_create_date,
                            a.Party_id as People_id,
                            a.Household_id,
                            b.phone_contact_id

                     FROM   tmp_bridge2 AS a LEFT OUTER JOIN
                            t_sda01.c_cdh_prty_phone_contact AS b

                     ON  a.Party_id = b.party_id
          """)

df_tb2_prty_ph.registerTempTable("tmp_bridge3")

#Joining tables tmp_bridge3 and c_cdh_phone_contact
df_tb3_ph_contact=hc.sql("""SELECT a.Customer_create_date,
                            a.People_id,
                            a.Household_id,
                            b.phone_area_cd, b.phone_num

                     FROM   tmp_bridge3 AS a LEFT OUTER JOIN
                            t_sda01.c_cdh_phone_contact AS b

                     ON  a.phone_contact_id = b.rowid_object
          """)

df_tb3_ph_contact.registerTempTable("tmp_bridge4")

#joining tables tmp_bridge4 and c_cdh_prty_email.
df_tb3_prty_email=hc.sql("""SELECT a.Customer_create_date,
                            a.People_id,
                            a.Household_id,
                            a.phone_area_cd,
                            a.phone_num,
                            b.email_id

             FROM   tmp_bridge4 AS a LEFT OUTER JOIN
                    t_sda01.c_cdh_prty_email AS b

		     ON     a.People_id = b.party_id
                        """)

df_tb3_prty_email.registerTempTable("tmp_bridge5")

#Joining tables tmp_bridge5 and c_cdh_email.
df_tb5_email=hc.sql("""SELECT a.Customer_create_date,
                            a.People_id,
                            a.Household_id,
                            a.phone_area_cd,
                            a.phone_num,
                            b.email_addr_nm as email_address

                     FROM   tmp_bridge5 AS a LEFT OUTER JOIN
                            t_sda01.c_cdh_email AS b

                     ON  a.email_id = b.rowid_object
                        """)

df_tb5_email.registerTempTable("tmp_bridge6")

df_tb6_party_addr = hc.sql("""SELECT a.Customer_create_date,
                            a.People_id,
                            a.Household_id,
                            a.phone_area_cd,
                            a.phone_num,
                            b.addr_id
                            a.email_addr_nm as email_address

                     FROM   tmp_bridge6 AS a LEFT OUTER JOIN
                            t_sda01.c_cdh_party_addr AS b

                     ON  a.People_id = b.party_id
                        """)

#Converting Data frame to an RDD.
my_rdd=df_tb5_email.rdd


#This function converts a full date(eg "2004-09-03 17:25:11") to a month date(eg "2004-09")
#@param DATE full_date
#@return String month_date
def month_extractor(full_date):
    #Storing first 8 characters into month_date variable
    month_date=full_date[0:7]
    return month_date

#Filtering the table based on the DateArg
monthly_rdd = my_rdd.filter(lambda x: month_extractor(x[0]) == DateArg)

#Converting back to data frame
s1=monthly_rdd.toDF()

#View the top 20 rows
s1.show()

print("Saving now")
#hc.sql("CREATE TABLE t_sda01.monthly_snapshot_table AS SELECT DISTINCT * FROM tmp_bridge6")

#Creating appropriate table name
Table_name = "t_sda01." + "sda_baby_bridge_snapshot_table_" + DateArg[0:4] + "_" + DateArg[5:7]

#Saving table to hive
s1.saveAsTable(Table_name, mode='overwrite')
