from pyspark.sql.functions import collect_list, collect_set, count, col, size, sum, udf, array, explode
from pyspark.sql.types import StringType, ArrayType

from __future__ import division
import string
import collections
from itertools import chain
import re
import timeit



#Bridge table
df=sqlContext.sql("""SELECT a.create_date as Customer_create_date,
                            f.rowid_object as person_id,
                            a.rowid_object AS Party_id,
                            c.rowid_object as household_id,
                            e.phone_area_cd, e.phone_num

                     FROM   t_sda01.c_cdh_party AS a,
                            t_sda01.c_cdh_household_prty_rel AS b,
                            t_sda01.c_cdh_household as c,
                            t_sda01.c_cdh_prty_phone_contact as d,
                            t_sda01.c_cdh_phone_contact as e,
                            t_sda01.c_cdh_person as f,


                     WHERE  a.rowid_object=b.rowid_cdh_party
                        AND b.rowid_cdh_household=c.rowid_object
                        AND a.rowid_object=d.party_id
                        AND d.phone_contact_id=e.rowid_object
                        AND a.rowid_object=f.party_id
                        """)

df.persist(StorageLevel.MEMORY_AND_DISK)
#df.cache()

df.registerTempTable("tmp_bridge")

my_rdd=df.rdd


#This function converts a full date(eg "2004-09-03 17:25:11") to a month date(eg "2004-09")
def month_extractor(full_date):
    month_date=full_date[0:7]
    return month_date


monthly_rdd = my_rdd.filter(lambda x: month_extractor(x[0])==sys.argv[1])
s1=monthly_rdd.toDF()
s1.show()                                                                                                                                                                       43,2          Bot






#date_rdd=my_rdd.map(lambda x: x[0])




#month_extractor(full_date="2004-09-03 17:27:11")
#date_rdd.map(lambda each_date:month_extractor(each_date)).take(5)

#my_rdd.map(lambda each_rec:month_extractor(each_rec[0])).take(5)
