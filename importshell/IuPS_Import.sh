#!/bin/sh

hdfs dfs -mkdir -p      /user/hive/warehouse/test.db/s_xdr_iups_attach_$1_$2/
hdfs dfs -mkdir -p      /user/hive/warehouse/test.db/s_xdr_iups_actpdp_$1_$2/
hdfs dfs -mkdir -p      /user/hive/warehouse/test.db/s_xdr_iups_pagserv_$1_$2/
hdfs dfs -mkdir -p      /user/hive/warehouse/test.db/s_xdr_iups_rau_$1_$2/
hdfs dfs -mkdir -p      /user/hive/warehouse/test.db/s_xdr_iups_modpdp_$1_$2/
hdfs dfs -mkdir -p      /user/hive/warehouse/test.db/s_xdr_iups_reloc_$1_$2/
hdfs dfs -mkdir -p      /user/hive/warehouse/test.db/s_xdr_iups_deactpdp_$1_$2/
hdfs dfs -mkdir -p      /user/hive/warehouse/test.db/s_xdr_iups_detach_$1_$2/
hdfs dfs -mkdir -p      /user/hive/warehouse/test.db/s_xdr_iups_iurelease_$1_$2/

cd $3/parseIuPS/
hdfs dfs -put IuPS_ATTACH/*       /user/hive/warehouse/test.db/s_xdr_iups_attach_$1_$2/
hdfs dfs -put IuPS_ACTPDP/*       /user/hive/warehouse/test.db/s_xdr_iups_actpdp_$1_$2/
hdfs dfs -put IuPS_PAGSERREQ/*    /user/hive/warehouse/test.db/s_xdr_iups_pagserv_$1_$2/
hdfs dfs -put IuPS_RAU/*          /user/hive/warehouse/test.db/s_xdr_iups_rau_$1_$2/
hdfs dfs -put IuPS_MODPDP/*       /user/hive/warehouse/test.db/s_xdr_iups_modpdp_$1_$2/
hdfs dfs -put IuPS_RELOC/*        /user/hive/warehouse/test.db/s_xdr_iups_reloc_$1_$2/
hdfs dfs -put IuPS_DEACTPDP/*     /user/hive/warehouse/test.db/s_xdr_iups_deactpdp_$1_$2/
hdfs dfs -put IuPS_DETACH/*       /user/hive/warehouse/test.db/s_xdr_iups_detach_$1_$2/
hdfs dfs -put IuPS_IURELEASE/*    /user/hive/warehouse/test.db/s_xdr_iups_iurelease_$1_$2/
