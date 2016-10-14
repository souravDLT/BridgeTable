from pyspark.sql.functions import *
from pyspark.sql.types import StringType, ArrayType

from __future__ import division
import string
import collections
from itertools import chain
import re
import timeit
import sys

hc=sqlContext


hc.sql("""SELECT count(rowid_object)
                  FROM t_sda01.c_cdh_household_prty_rel
                   """).show()

+--------+
|22502777|
+--------+

df=hc.sql("""SELECT *
                  FROM t_sda01.c_cdh_household_prty_rel
                   """)
df.persist(StorageLevel.MEMORY_AND_DISK)

hc.sql("""SHOW CREATE TABLE t_sda01.c_cdh_household_prty_rel
                   """).show()

filtereddf = hc.sql("""SELECT rowid_object,
                 rowid_cdh_household,
                 rowid_cdh_party,
                 last_update_date
                FROM t_sda01.c_cdh_household_prty_rel
              order by last_update_date DESC
                   """)
filtereddf.persist(StorageLevel.MEMORY_AND_DISK)
filtereddf.show()
--------------------------------------------------------------------------------
#real stuff

filtereddf1 = hc.sql("""SELECT rowid_object,
                               rowid_cdh_household,
                               rowid_cdh_party,
                               last_update_date
                        FROM   t_sda01.c_cdh_household_prty_rel
                        WHERE  last_update_date < '2014-10-09 10:10:01'
                        ORDER BY last_update_date DESC
                   """)

filtereddf1.show()
filtereddf1.persist(StorageLevel.MEMORY_AND_DISK)
filtereddf1.registerTempTable("table1")
#df=filtereddf1.groupBy("rowid_cdh_party")       #.count().collect()

windowdf = hc.sql("""SELECT rowid_cdh_household,
                            rowid_cdh_party,
                            last_update_date,
                            rank
                     FROM (
                           SELECT   rowid_cdh_household,
                                    rowid_cdh_party,
                                    last_update_date,
                                    rank()
                           OVER (PARTITION BY rowid_cdh_party
                           ORDER BY last_update_date DESC) AS rank
                           FROM t_sda01.c_cdh_household_prty_rel
                           WHERE  last_update_date < '2014-10-09 10:10:01') AS tmp
                    WHERE  rank = 1
                 """)
windowdf.show()
#count
20943536
windowdf.registerTempTable("windowTable")

--------------------------------------------------------------------------------
hc.sql("""
       SELECT rowid_cdh_household, first(rank), count(rowid_cdh_party) as ct
       FROM   windowTable
       GROUP  BY rowid_cdh_household
       ORDER BY ct DESC
       """).show()
       WHERE rowid_cdh_party=53185721 83685968
+---+---------------+
| 12| 83685968      |
| 12| 84018115      |
| 11| 83805711      |
| 10| 86687151      |
| 10| 68961170      |
|  9| 84103048      |
|  8| 82927772      |
|  8| 86587516      |
|  8| 89594041      |
|  8| 61538584      |
|  7| 63854364      |
|  7| 88329983      |
|  7| 89414442      |

|  7| 54191756      |
|  7| 87167525      |
|  7| 61753493      |
|  7| 84247243      |
|  7| 83661306      |
|  7| 82426879      |
|  7| 66620093      |
+---+---------------+
--
--
--
------------
hc.sql("""
       SELECT rowid_cdh_party, rowid_cdh_household, count(rowid_cdh_party, rowid_cdh_household) AS cnt
       FROM   Table1
       GROUP  BY rowid_cdh_party, rowid_cdh_household
       ORDER BY cnt DESC
       """).show()

| ct|rowid_cdh_party|
+---+---------------+
| 44| 92145577      |
| 24| 83685968      |
| 16| 89130134      |
| 14| 84018115      |
| 14| 75949260      |
| 13| 75445846      |
| 13| 84178350      |
| 12| 68961170      |
| 12| 83805711      |
| 12| 86687151      |
| 11| 83779420      |

| 11| 56470298      |
| 11| 82927772      |
| 10| 85998626      |
| 10| 86587516      |
| 10| 92214684      |
| 10| 84103048      |
| 10| 83563595      |
| 10| 90552321      |
| 10| 84209802      |
+---+---------------+


hc.sql("""
       SELECT DISTINCT rowid_cdh_party, rowid_cdh_household, last_update_date
       FROM   windowTable
       WHERE rowid_cdh_party=84018115
       AND rowid_cdh_household=45789408
       """).show()



+---------------+-------------------+-------------------+
|rowid_cdh_party|rowid_cdh_household|   last_update_date|
+---------------+-------------------+-------------------+
| 53185721      |     46443811      |2014-01-17 13:33:04|
| 53185721      |     29680862      |2014-01-13 10:10:23|

+---------------+-------------------+-------------------+



hc.sql("""
       SELECT rowid_cdh_party, rowid_cdh_household, last_update_date
       FROM   windowTable
       WHERE rowid_cdh_party=53185721
       """).show()
--------------------------------------------------------------------------------
#inner query

      inner1=hc.sql(""" SELECT   rowid_cdh_household,
                 rowid_cdh_party,
                last_update_date,
                rank()
       OVER (PARTITION BY rowid_cdh_party
       ORDER BY last_update_date DESC) AS rank
       FROM table1
       ORDER BY rank DESC
        """)


      hc.sql(""" SELECT   rowid_cdh_household,
                 rowid_cdh_party,
                last_update_date,
                rank

       FROM windowTable
       ORDER BY rank DESC
        """).show()

inner1.show()

hc.sql("""SELECT  rowid_cdh_household,
                  rowid_cdh_party,
                  last_update_date
          FROM t_sda01.c_cdh_household_prty_rel
          WHERE rowid_cdh_household = 46443811
          AND rowid_cdh_party = 53185721
          ORDER BY rowid_cdh_party DESC
                   """).show()

+-------------------+---------------+-------------------+
|rowid_cdh_household|rowid_cdh_party|   last_update_date|
+-------------------+---------------+-------------------+
|     30558769      | 53182795      |2011-10-09 14:46:51|
|     29897084      | 53183169      |2011-10-09 14:38:31|
|     42461486      | 53183194      |2013-03-25 14:56:00|
|     38968737      | 53183194      |2013-03-25 14:55:59|
|     36830943      | 53183727      |2011-10-09 13:41:40|
|     33366989      | 53183752      |2011-10-09 16:05:12|
|     33343229      | 53184126      |2011-10-09 14:38:31|
|     30135147      | 53184379      |2011-10-09 14:42:30|
|     37287315      | 53184445      |2011-10-09 14:22:12|
|     31276574      | 53184470      |2011-10-09 16:42:44|

|     31451937      | 53185097      |2011-10-09 15:40:23|
|     33844734      | 53185402      |2011-10-09 14:30:11|
|     30551504      | 53185655      |2011-10-09 13:32:21|
|     45587575      | 53185680      |2013-11-15 11:06:45|
|     30350708      | 53185680      |2013-11-15 11:06:45|
|     46443811      | 53185721      |2014-01-17 13:33:04|
|     38999633      | 53185974      |2011-10-09 14:58:32|
|     37010531      | 53186029      |2011-10-09 16:42:44|
|     34610347      | 53186120      |2011-10-09 14:58:32|
|     32265041      | 53186348      |2011-10-09 14:58:32|
+-------------------+---------------+-------------------+
only showing top 20 rows

hc.sql("""
       SELECT rowid_object, last_update_date
       FROM   t_sda01.c_cdh_party
       WHERE rowid_object=84018115
       """).show()

--------------------------------------------------------------------------------
#Handy commands
#displays the data types
filtereddf1.dtypes()


filtereddf1.printSchema()


filtereddf1.distinct().count()
21742474

filtereddf1.drop_duplicates().count()
21742474

#gives logical and physical plan
filtereddf1.explain()

filtereddf1.groupby("rowid_cdh_party")

#not working
display(filtereddf1.limit(10))


filtereddf1.groupBy("rowid_cdh_party").count().collect()

type(filtereddf1)
