from pyspark.sql.functions import collect_list, collect_set, count, col, size, sum, udf, array, explode
from pyspark.sql.types import StringType, ArrayType

from __future__ import division
import string
import collections
from itertools import chain
import re
import timeit
import sys

df4=sqlContext.sql("""SELECT a.create_date as Customer_create_date,
                            a.rowid_object AS Party_id,
                            b.rowid_cdh_household

                     FROM   t_sda01.c_cdh_party AS a LEFT OUTER JOIN
                            t_sda01.c_cdh_household_prty_rel AS b

                     ON  a.rowid_object=b.rowid_cdh_party
                        """)
        df4.persist(StorageLevel.MEMORY_AND_DISK)
        #df.cache()

        df4.registerTempTable("tmp_bridge1")


        df5=sqlContext.sql("""SELECT a.Customer_create_date,
                                    a.Party_id,
                                    b.rowid_object AS Household_id


                             FROM   tmp_bridge1 AS a LEFT OUTER JOIN
                                    t_sda01.c_cdh_household AS b

                             ON  a.rowid_cdh_household = b.rowid_object
                                """)

            df5.registerTempTable("tmp_bridge2")


df6=sqlContext.sql("""SELECT a.Customer_create_date,
                            a.Party_id as People_id,
                            a.Household_id,
                            b.phone_contact_id

                     FROM   tmp_bridge2 AS a LEFT OUTER JOIN
                            t_sda01.c_cdh_prty_phone_contact AS b

                     ON  a.Party_id = b.party_id
                        """)

    df6.registerTempTable("tmp_bridge3")

df7=sqlContext.sql("""SELECT a.Customer_create_date,
                            a.People_id,
                            a.Household_id,
                            b.phone_area_cd, b.phone_num

                     FROM   tmp_bridge3 AS a LEFT OUTER JOIN
                            t_sda01.c_cdh_phone_contact AS b

                     ON  a.phone_contact_id = b.rowid_object
                        """)

    df7.registerTempTable("tmp_bridge4")
#done till phone----------------------------------------------------------------

df8=sqlContext.sql("""SELECT a.Customer_create_date,
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

df9=sqlContext.sql("""SELECT a.Customer_create_date,
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


monthName = input('Enter the month(YYYY-MM):    ')
matchObj1 = re.match( r'[0-9]{4}[-][0][1-9]|[0-9]{4}[-][1][0-2]', monthName)
if matchObj1:
    monthName1=monthName[0:7]
    print (monthName1)
    monthly_rdd = my_rdd.filter(lambda x: month_extractor(x[0]) == monthName)
    s1=monthly_rdd.toDF()
    s1.show()

else
    print('Invalid input')
    sys.exit(0)

print("\n Do you want to save this snapshot? (y/n)  :")
saveInput = input()
matchObj2 = re.match( r'[y]|[n]', monthName)
if saveInput[0]=="y":
        s1.saveAsTable("t_sda01.MonthlySnapshotBridge", mode='overwrite')
elif saveInput[0]=="n":
        print("GoodBye")
else:
        print("Invalid input")

# [0-9]{4}[-][0][1-9]|[0-9]{4}[-][1][0-2]

#s1.saveAsTable("MonthlySnapshotBridge", mode='overwrite', path='/team/sda/Sourav')

--------------------------------------------------------------------------------
sqlContext.sql("""SELECT count(rowid_object)
                  FROM t_sda01.c_cdh_party
                   """).show()
|782299|

d1=sqlContext.sql("""SHOW CREATE TABLE t_sda01.c_cdh_party
                   """).show()


sqlContext.sql("""SELECT count(Party_id)
                  FROM tmp_bridge2
                   """).show()
|1927303|

sqlContext.sql("""SELECT count(People_id)
                  FROM tmp_bridge3
                   """).show()
|3410815|

sqlContext.sql("""Describe tmp_bridge3
                   """).show()

sqlContext.sql("""SELECT count(*) from tmp_bridge4
                   """).show()
|5951171|    inner
|6740793|    outer

sqlContext.sql("""SELECT count(People_id)
                  FROM tmp_bridge5
                   """).show()
|12233958|  outer

sqlContext.sql("""SELECT count(People_id)
                  FROM tmp_bridge6
                   """).show()
|37295609|  outer

sqlContext.sql("""SELECT count(*)
                  FROM tmp_bridge6
                   """).show()
|37295609|

sqlContext.sql("""SELECT distinct count(*)
                  FROM tmp_bridge6
                   """).show()
|37295609|

sqlContext.sql("""SELECT  distinct *
                  FROM tmp_bridge6
                  ORDER BY Customer_create_date
                   """).show()
|Customer_create_date|     People_id|  Household_id|phone_area_cd|phone_num|       email_address|
+--------------------+--------------+--------------+-------------+---------+--------------------+
| 2004-09-03 17:24:32|65217599      |          null|         null|     null|                null|
| 2004-09-03 17:24:39|56447904      |31787444      |          765|  5247953|    cpetty@amfam.com|
| 2004-09-03 17:24:39|56447904      |31787444      |          765|  5247953|    cpetty@amfam.com|
| 2004-09-03 17:24:39|56447904      |31787444      |          765|  5247953|    cpetty@amfam.com|
| 2004-09-03 17:24:39|56447904      |31787444      |          765|  5247953|    cpetty@amfam.com|
| 2004-09-03 17:24:39|56447904      |31787444      |          765|  5247953|    cpetty@amfam.com|
| 2004-09-03 17:24:39|56447904      |31787444      |          765|  5247953|    cpetty@amfam.com|
| 2004-09-03 17:24:39|65168961      |33880874      |          970|  6833306|colleen-wilson@br...|
| 2004-09-03 17:24:39|53291274      |32466748      |          952|  9465724|  sabail07@gmail.com|
| 2004-09-03 17:24:39|56447904      |31787444      |          765|  5247953|    cpetty@amfam.com|
| 2004-09-03 17:24:39|56447904      |31787444      |          765|  5247953|    cpetty@amfam.com|
| 2004-09-03 17:24:39|56447904      |31787444      |          765|  5247953|    cpetty@amfam.com|
| 2004-09-03 17:24:39|56447904      |31787444      |          765|  5247953|    cpetty@amfam.com|
| 2004-09-03 17:24:39|65168961      |33880874      |          970|  2417882|colleen-wilson@br...|
| 2004-09-03 17:24:39|65168961      |33880874      |          970|  2609684|colleen-wilson@br...|
| 2004-09-03 17:24:39|56447904      |31787444      |          765|  5247953|    cpetty@amfam.com|
| 2004-09-03 17:24:39|53291274      |32466748      |          651|  4544015|  sabail07@gmail.com|
| 2004-09-03 17:24:39|53291274      |32466748      |          612|  3278032|  sabail07@gmail.com|
| 2004-09-03 17:24:39|56447904      |31787444      |          765|  5247953|    cpetty@amfam.com|
| 2004-09-03 17:24:39|65979248      |34966434      |          970|  4982350|  dbanks@cppwind.com|



sqlContext.sql("""SELECT  *
                  FROM tmp_bridge6
                  ORDER BY Customer_create_date DESC
                   """).show()

|Customer_create_date|     People_id|  Household_id|phone_area_cd|phone_num|  email_address|
+--------------------+--------------+--------------+-------------+---------+---------------+
| 2014-07-16 11:38:02|91483293      |48644383      |          970|  9033950|chipm@plpoa.com|
| 2014-07-16 11:38:02|91483293      |          null|          970|  9033950|chipm@plpoa.com|
| 2014-07-16 00:00:00|91496555      |30535948      |         null|     null|           null|
| 2014-07-16 00:00:00|91496576      |31470347      |         null|     null|           null|
| 2014-07-16 00:00:00|91496570      |48513808      |         null|     null|           null|
| 2014-07-16 00:00:00|91496576      |31470347      |         null|     null|           null|
| 2014-07-16 00:00:00|91496557      |36775561      |         null|     null|           null|
| 2014-07-16 00:00:00|91496579      |48653138      |         null|     null|           null|
| 2014-07-16 00:00:00|91496557      |36775561      |         null|     null|           null|
| 2014-07-16 00:00:00|91496560      |48653143      |         null|     null|           null|
| 2014-07-16 00:00:00|91496576      |31470347      |         null|     null|           null|
| 2014-07-16 00:00:00|91496551      |45554387      |         null|     null|           null|
| 2014-07-16 00:00:00|91496578      |48653133      |         null|     null|           null|
| 2014-07-16 00:00:00|91496568      |44418621      |         null|     null|           null|
| 2014-07-16 00:00:00|91496580      |48653151      |         null|     null|           null|
| 2014-07-16 00:00:00|91496555      |30535948      |         null|     null|           null|
| 2014-07-16 00:00:00|91496577      |48653147      |         null|     null|           null|
| 2014-07-16 00:00:00|91496553      |39991473      |         null|     null|           null|
| 2014-07-16 00:00:00|91496584      |37299984      |         null|     null|           null|
| 2014-07-16 00:00:00|91496559      |          null|         null|     null|           null|
+--------------------+--------------+--------------+-------------+---------+---------------+
--------------------------------------------------------------------------------
LEFT OUTER JOIN
t_sda01.c_cdh_prty_phone_contact as d INNER JOIN
t_sda01.c_cdh_phone_contact as e RIGHT OUTER JOIN
t_sda01.c_cdh_person as f
