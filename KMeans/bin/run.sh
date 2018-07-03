#!/bin/bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/config.sh"
. "${DIR}/bin/config.sh"

echo "========== running ${APP} bench =========="


# pre-running
DU ${INPUT_HDFS} SIZE 

JAR="${DIR}/target/KMeansApp-1.0.jar"
CLASS="KmeansApp"
#OPTION=" ${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS} ${NUM_OF_CLUSTERS} ${MAX_ITERATION} ${NUM_RUN}"
OPTION=" ${INOUT_SCHEME}${INPUT_HDFS} 2 8 0"
#OPTION0=" ${INOUT_SCHEME}${INPUT_HDFS} 0 "
#MY_OPT=" --num-executors 32 "


setup
#for((i=0;i<${NUM_TRIALS};i++)); do
for MY in 0 16 ;
#for MY in 0 0 16 16 24 24 28 28 30 30 31 31;
#for MY in 2 4 6 8 10 12 14 16 18 20 22 24 26 28 30;
#for MY in  0;
do
    START_TS=`get_start_ts`;
	  START_TIME=`timestamp`
    /root/ephemeral-hdfs/bin/hadoop dfs -rmr /SparkBench/KMeans/Output*
    /root/ephemeral-hdfs/bin/hadoop dfs -rmr /SparkBench/PageRank/Output*
    /root/ephemeral-hdfs/bin/hadoop dfs -rmr /SparkBench/SVDPlusPlus/Output*
    /root/ephemeral-hdfs/bin/hadoop dfs -rmr /SparkBench/SVM/Output*
    /root/ephemeral-hdfs/bin/hadoop dfs -rmr /SparkBench/ConnectedComponent/Output*
    /root/ephemeral-hdfs/bin/hadoop dfs -rmr /SparkBench/PCA/Output*
    /root/ephemeral-hdfs/bin/hadoop dfs -rmr /SparkBench/DecisionTree/Output*
    /root/ephemeral-hdfs/bin/hadoop dfs -rmr /SparkBench/PregelOperation/Output*
    /root/ephemeral-hdfs/bin/hadoop dfs -rmr /SparkBench/LogisticRegression/Output*
    OPTION=" ${INOUT_SCHEME}${INPUT_HDFS} 1 ${MY} 0"
#    OPTION=" ${INOUT_SCHEME}${INPUT_HDFS} 1 16 0"
#    OPTION=" ${INOUT_SCHEME}${INPUT_HDFS} 4"
    purge_data "${MC_LIST}"	
#    echo_and_run sh -c " ${SPARK_HOME}/bin/spark-submit --class $CLASS --master ${APP_MASTER} ${YARN_OPT} ${SPARK_OPT} ${SPARK_RUN_OPT} $JAR ${OPTION} 2&1> ${BENCH_NUM}/${APP}_run_${START_TS}.dat"
#    echo_and_run sh -c " ${SPARK_HOME}/bin/spark-submit --class $CLASS --master ${APP_MASTER} ${YARN_OPT} ${SPARK_OPT} ${SPARK_RUN_OPT} ${MY_OPT} --queue default $JAR ${OPTION0} 2>&1|tee ${BENCH_NUM}/${APP}_run_${START_TS}.dat" &
#    sleep 40
    echo_and_run sh -c " ${SPARK_HOME}/bin/spark-submit --class $CLASS --master ${APP_MASTER} ${YARN_OPT} ${SPARK_OPT} ${SPARK_RUN_OPT} $JAR ${OPTION} 2>&1|tee ${BENCH_NUM}/${APP}_run_${START_TS}.dat"
#    echo_and_run sh -c " ${SPARK_HOME}/bin/spark-submit --class $CLASS --master ${APP_MASTER} ${YARN_OPT} ${SPARK_OPT} ${SPARK_RUN_OPT} ${MY_OPT} --queue b $JAR ${OPTION} 2>&1|tee ${BENCH_NUM}/${APP}_run_${START_TS}.dat" &

done
teardown

exit 0

