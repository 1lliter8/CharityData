# Notes

Just a place to keep stuff for now.

#### Timings

As of 17/6's commit, running on my potato machine.

* extract_aoo_ref.bcp: instant
* extract_charity.bcp: 7 minutes 34 seconds
* extract_name.bcp: 4 minutes 45 seconds
* extract_main_charity.bcp: 2 minutes 42 seconds
* extract_class_ref.bcp: instant
* extract_remove_ref.bcp: instant
* extract_acct_submit.bcp: 10 minutes 1 second
* extract_ar_submit.bcp: 18 minutes	20 seconds
* extract_charity_aoo.bcp: roughly 5 mins
* extract_class.bcp: roughly 15 mins

##### Speed testing later on 17/6

* extract_name with from_dict: 4.45
    * Before SQL: 1.36
* extract_name with from_record: 4.36
    * Before SQL: 1.34
    * Total, using odo: 4.16
* extract_name with from_record, multi: 7.51
* extract_name with from_record, multi, 10k chunksize: 8.04
* extract_financial with from_dict:
    * Before SQL: 8:12
    * Total: 26:18
* extract_financial with from_record:
    * Before SQL: 7.45
    * Total: 26:56
    
##### d6tstack tests

* extract_name with from_record: 
    * Original, df.to_sql: 4.36
    * Before SQL: 1.34
    * Total, using d6 method: 2.11
    * Total, using d6 without pandas: 46 seconds
    
##### self-written d6 methods, dataframe removed

FIRST EVER COMPLETION!!!!!

Total: 13 minutes 21 seconds

    `Import begun at 2020-06-19 18:39:22.458818
    extract_aoo_ref.bcp, 2020-06-19 18:39:22.506801: Successfully inserted
    extract_charity.bcp, 2020-06-19 18:39:57.618834: Successfully inserted
    extract_name.bcp, 2020-06-19 18:40:31.165849: Successfully inserted
    extract_main_charity.bcp, 2020-06-19 18:40:45.505787: Successfully inserted
    extract_class_ref.bcp, 2020-06-19 18:40:45.527802: Successfully inserted
    extract_remove_ref.bcp, 2020-06-19 18:40:45.539782: Successfully inserted
    extract_acct_submit.bcp, 2020-06-19 18:41:52.291268: Successfully inserted
    extract_ar_submit.bcp, 2020-06-19 18:44:11.807851: Successfully inserted
    extract_charity_aoo.bcp, 2020-06-19 18:44:40.201848: Successfully inserted
    extract_class.bcp, 2020-06-19 18:47:03.768491: Successfully inserted
    extract_financial.bcp, 2020-06-19 18:50:02.314364: Successfully inserted
    extract_objects.bcp, 2020-06-19 18:50:59.870691: Successfully inserted
    extract_partb.bcp, 2020-06-19 18:51:25.821685: Successfully inserted
    extract_registration.bcp, 2020-06-19 18:51:51.917320: Successfully inserted
    extract_trustee.bcp, 2020-06-19 18:52:43.070598: Successfully inserted
    Import finished at 2020-06-19 18:52:43.095612`