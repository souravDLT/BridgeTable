hc=sqlContext


df = hc.sql("""SELECT updated_on
                  FROM p_policy_pl_auto_classic.auto_policy_hist
                   """).show(20, False)
df.cache()


hc.sql("""SELECT updated_on
                  FROM p_policy_pl_auto_classic.auto_policy_hist
                  WHERE amplan_acct_num=0.00000000
                   """).show(500, False)

hc.sql("""SHOW CREATE TABLE p_policy_pl_auto_classic.auto_policy_hist
      """).show(500)



ap_actv_time
ap_actv_date
comp_ap_actv_date
comp_ap_actv_time
comp_cov_chg_date

amplan_acct_num, two_pay_amt_due everything is 0.000000

move_to_offline_date everything is 99999999
0,0 reas_oth_canc_lien_termin_date everything is blank
boat_model_yr1 0's and NULLs both are present
crn_credit_rps_timestamp dates are fine
