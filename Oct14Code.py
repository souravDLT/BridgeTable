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
#No. of rows
|20871340|



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


#No. of rows
|664167|
windowdf1.show()
windowdf1.registerTempTable("windowTable1")
--------------+----------------+-------------------+----+
|53188023      |  37232937      |2014-02-13 04:27:31|   1|
|53190808      |  30624863      |2014-03-01 00:46:31|   1|
|53192761      |  45412726      |2014-03-01 00:47:38|   1|
|53207144      |  34568213      |2014-03-01 23:44:46|   1|
|53209883      |  40072143      |2014-03-01 02:12:39|   1|
|53225720      |  36747820      |2014-03-20 22:47:48|   1|
|53234704      |  35281750      |2014-02-13 02:08:04|   1|
|53243647      |  43790342      |2014-02-13 00:00:01|   1|
|53249128      |  32164303      |2014-02-21 00:51:42|   1|
|53253080      |  33848940      |2014-07-09 09:05:25|   1|
|53255024      |  43378346      |2014-03-02 00:20:16|   1|
|53255024      |  39603709      |2014-03-02 00:20:16|   1|
|53255024      |  38830772      |2014-03-02 00:20:16|   1|
|53266453      |  39086891      |2013-09-27 08:23:24|   1|

|53281014      |  43076139      |2014-02-15 00:32:40|   1|
|53282796      |  41001390      |2014-03-05 00:43:02|   1|
|53295039      |  37420561      |2014-03-05 02:08:48|   1|
|53296568      |  47070233      |2014-03-02 14:54:27|   1|
|53297286      |  32526542      |2014-02-26 00:58:47|   1|
|53304007      |  39614615      |2014-05-27 09:01:26|   1|
+--------------+----------------+-------------------+----+

#Joining 2 tables

JoinWDF12=hc.sql("""
                 SELECT rowid_cdh_party,
                        rowid_cdh_household AS Household_id,
                        phone_contact_id


                 FROM    windowTable AS a FULL OUTER JOIN windowTable1 AS b
                 ON party_id = rowid_cdh_party
                 """)

JoinWDF12.show()
JoinWDF12.registerTempTable("JoinedWindow12")





hc.sql("""
       SELECT party_id, phone_contact_id, count(party_id, phone_contact_id) AS cnt
       FROM   windowTable1
       GROUP  BY party_id, phone_contact_id
       ORDER BY cnt DESC
       """).show()
+--------------+----------------+---+
|      party_id|phone_contact_id|cnt|
+--------------+----------------+---+
|61328711      |  39437471      | 10|




hc.sql("""
       SELECT DISTINCT party_id, phone_contact_id, last_update_date
       FROM   windowTable1
       WHERE party_id=61328711
       AND phone_contact_id=39437471
       """).show()



+--------------+----------------+-------------------+
|      party_id|phone_contact_id|   last_update_date|
+--------------+----------------+-------------------+
|61328711      |  39437471      |2014-05-16 01:11:52|
+--------------+----------------+-------------------+


windowdf3 = hc.sql("""SELECT  DISTINCT party_id,
                              email_id,
                              last_update_date,
                              rank
                      FROM (
                           SELECT   party_id,
                                    email_id,
                                    last_update_date,
                                    rank()
                           OVER (PARTITION BY party_id
                           ORDER BY last_update_date DESC) AS rank
                           FROM t_sda01.c_cdh_prty_email
                           WHERE  last_update_date < '2012-10-09 10:10:01') AS tmp
                    WHERE  rank = 1
                 """)

windowdf3.show()
windowdf3.registerTempTable("windowTable3")

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
--------------------------------------------------------------------------------
#test

hc.sql("""
       SELECT count(*)
       FROM windowTable
       ON rowid_cdh_party ==windowTable1.party_id
       """).show()


hc.sql("""
       SELECT count(*)
       FROM windowTable LEFT OUTER JOIN windowTable1
       ON windowTable.rowid_cdh_party ==windowTable1.party_id
       """).show()
       |21068680|

hc.sql("""
       SELECT count(*)
       FROM windowTable FULL OUTER JOIN windowTable1
       ON windowTable.rowid_cdh_party ==windowTable1.party_id
       """).show()
       |21094152|

hc.sql("""
       SELECT count(*)
       FROM windowTable RIGHT OUTER JOIN windowTable1
       ON windowTable.rowid_cdh_party ==windowTable1.party_id
       """).show()
       |668100|

hc.sql("""
       SELECT count(*)
       FROM windowTable INNER JOIN windowTable1
       ON windowTable.rowid_cdh_party =windowTable1.party_id
       """).show()
       |642628|

hc.sql("""
       SELECT *
       FROM JoinedWindow123
       WHERE email_id is NOT NULL
       AND rowid_cdh_party is NOT NULL
       AND phone_contact_id is NOT NULL
       """).show()
|280113|
|rowid_cdh_party|  Household_id|phone_contact_id|      email_id|
+---------------+--------------+----------------+--------------+
| 62225792      |36782599      |  31181892      |8480283       |
| 62225792      |36782599      |  31559311      |8480283       |
| 62225792      |36782599      |  34633787      |8480283       |
| 62225792      |36782599      |  35780146      |8480283       |
| 67162605      |31414801      |  35468791      |9902802       |
| 54247447      |31891443      |  35746028      |9100408       |
| 62582521      |34295269      |  38266125      |9645475       |
| 75794522      |40972323      |  42441253      |11846295      |
| 55765427      |38993024      |  41260962      |11144082      |

| 67028100      |33457880      |  36834652      |10603040      |
| 67028100      |33457880      |  36743876      |10603040      |
| 53318496      |36108599      |  37604312      |10208822      |
| 53318496      |36108599      |  38884109      |10208822      |
| 54893341      |30881912      |  38676396      |9330879       |
| 56225683      |29628568      |  35035973      |9287920       |
| 56225683      |29628568      |  39305530      |9287920       |
| 57453805      |35657160      |  36091094      |9240036       |


hc.sql("""
       SELECT *
       FROM JoinedWindow123
       WHERE Household_id =39401249
       AND rowid_cdh_party =53350754
       """).show()

hc.sql("""
       SELECT last_update_date
       FROM t_sda01.c_cdh_prty_phone_contact
       WHERE party_id=67028100
       AND phone_contact_id=36834652
       """).show()
    |2014-03-01 02:12:39|
