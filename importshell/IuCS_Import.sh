#!/bin/sh

hdfs dfs -mkdir -p      /user/hive/warehouse/test.db/s_xdr_iucs_call_$1_$2/
hdfs dfs -mkdir -p      /user/hive/warehouse/test.db/s_xdr_iucs_sms_$1_$2/
hdfs dfs -mkdir -p      /user/hive/warehouse/test.db/s_xdr_iucs_lau_$1_$2/
hdfs dfs -mkdir -p      /user/hive/warehouse/test.db/s_xdr_iucs_ho_$1_$2/
hdfs dfs -mkdir -p      /user/hive/warehouse/test.db/s_xdr_iucs_pag_$1_$2/

cd $3/parseIuCS/
hdfs dfs -put CALL/*    /user/hive/warehouse/test.db/s_xdr_iucs_call_$1_$2/
hdfs dfs -put SMS/*     /user/hive/warehouse/test.db/s_xdr_iucs_sms_$1_$2/
hdfs dfs -put LAU/*     /user/hive/warehouse/test.db/s_xdr_iucs_lau_$1_$2/
hdfs dfs -put HO/*      /user/hive/warehouse/test.db/s_xdr_iucs_ho_$1_$2/
hdfs dfs -put PAG/*     /user/hive/warehouse/test.db/s_xdr_iucs_pag_$1_$2/
