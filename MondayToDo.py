#CODE: Baby bridge Table
#This Pyspark application creates a BabyBridge Table
#Program Input: Date in the format (YYYY-MM) given as a CL Argument with spark-submit
#Program Output: A hive table called snapshot_table_<YYYY>_<MM>
#Author: Sourav Bhowmik, Data Lab Team
#Last Modified: Oct 11, 2016
#spark-submit --master yarn-client driverprog.py "2010-05"

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
if (len(DateArg) == 7):
    if (matchObj1):
        print("Hi, correct argument")

else:
        print('Invalid input, please try again in (YYYY-MM) format. \nTerminating Program..........')
        #Terminate program in case DateArg value is invalid.
        sys.exit(0)

#Initializing SparkContext and HiveContext.
conf = SparkConf().setAppName('Mini Bridge Table').set("spark.executor.memory", "64g").set("spark.driver.memory", "32g")
sc = SparkContext(conf = conf)
hc = HiveContext(sc)



windowdf = hc.sql("""SELECT DISTINCT rowid_cdh_household,
                            rowid_cdh_party,
                            last_update_date
                     FROM (
                           SELECT   rowid_cdh_household,
                                    rowid_cdh_party,
                                    last_update_date,
                                    rank()
                           OVER (PARTITION BY rowid_cdh_party
                           ORDER BY last_update_date DESC) AS rank
                           FROM t_sda01.c_cdh_household_prty_rel
                           WHERE  last_update_date < '2012-10-09 10:10:01') AS tmp
                    WHERE  rank = 1
                 """)
windowdf.registerTempTable("windowTable")
windowdf.cache()

windowdf1 = hc.sql("""SELECT  DISTINCT party_id,
                              phone_contact_id,
                              last_update_date
                      FROM (
                           SELECT   party_id,
                                    phone_contact_id,
                                    last_update_date,
                                    rank()
                           OVER (PARTITION BY party_id
                           ORDER BY last_update_date DESC) AS rank
                           FROM t_sda01.c_cdh_prty_phone_contact
                           WHERE  last_update_date < '2012-10-09 10:10:01') AS tmp
                    WHERE  rank = 1
                 """)




windowdf1.show()
windowdf1.registerTempTable("windowTable1")

JoinWDF12=hc.sql("""
                 SELECT rowid_cdh_party,
                        rowid_cdh_household AS Household_id,
                        phone_contact_id


                 FROM    windowTable AS a FULL OUTER JOIN windowTable1 AS b
                 ON party_id = rowid_cdh_party
                 """)

JoinWDF12.show()
JoinWDF12.registerTempTable("JoinedWindow12")


JoinWDF123=hc.sql("""
                 SELECT rowid_cdh_party,
                        Household_id,
                        phone_contact_id,
                        email_id


                 FROM    JoinedWindow12 AS a FULL OUTER JOIN windowTable3 AS b
                 ON party_id = rowid_cdh_party
                 """)

JoinWDF123.show()
JoinWDF123.registerTempTable("JoinedWindow123")

#-------------------------------------------------------------------------------
#done till email
