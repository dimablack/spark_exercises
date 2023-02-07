#!/usr/bin/env bash

Help()
{
    echo "Download and setup local Spark"
    echo
    echo "Syntax: hardcore_setup.sh -c <num_cpu> -m <memory_mb> [-d]"
    echo "options:"
    echo "c     number of CPUs for Spark"
    echo "m     Memory for Spark (in MB)"
    echo "d     (Optional) if you want to delete and re-download"
    echo
}

while getopts ":c:m:d" option; do
    case $option in
        c)
            NUM_CPUS=$OPTARG;;
        m)
            AVAILABLE_MEMORY_MB=$OPTARG;;
        d)
            CLEAN_DOWNLOAD="yes";;
        \?)
            echo "Invalid option."
            Help
            exit;;
    esac
done

if [[ -z $NUM_CPUS || -z $AVAILABLE_MEMORY_MB ]]; then
    Help
    exit
fi

SPARK_VERSION=3.3.1
HADOOP_VERSION=3
SCRIPT_DIR=$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )
SPARK_HOME="${SCRIPT_DIR}/spark"

if [[ $CLEAN_DOWNLOAD || ! -d $SPARK_HOME ]]; then
    if [[ -d $SPARK_HOME ]]; then rm -rf ${SPARK_HOME}; fi
    mkdir -p ${SPARK_HOME};
    echo "Downloading Spark ${SPARK_VERSION}..."
    curl "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" |\
        tar -xz -C ${SPARK_HOME} --strip=1
fi

cat <<-EOF > ${SCRIPT_DIR}/spark.sh
export SPARK_HOME="${SPARK_HOME}"
export PATH="${SPARK_HOME}/bin:${PATH}"
export PYSPARK_PYTHON="${SCRIPT_DIR}/venv/bin/python"

alias s-shell="\${SPARK_HOME}/bin/pyspark --master 'local[$NUM_CPUS]' --driver-memory ${AVAILABLE_MEMORY_MB}M"
alias s-submit="\${SPARK_HOME}/bin/spark-submit --master 'local[$NUM_CPUS]' --driver-memory ${AVAILABLE_MEMORY_MB}M"
EOF



