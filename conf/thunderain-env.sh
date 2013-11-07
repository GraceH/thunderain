#!/bin/sh
#export DATA_CLEAN_TTL=10
export BATCH_DURATION=1

# kafka configuration
export ZK_QUORUM=10.0.2.12:2181
export KAFKA_GROUP=test
export KAFKA_INPUT_NUM=6

# tachyon configuration
export TACHYON_MASTER=Gracehost:18998
export TACHYON_WAREHOUSE_PATH=/user/tachyon

# hdfs configuration
export HDFS_PATH=hdfs://localhost:54310/test

# mongoDB configuration
export MONGO_ADDRESSES=mongodb://localhost:27017
export MONGO_DB=test
