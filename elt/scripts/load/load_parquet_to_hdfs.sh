#!/bin/bash
set -e

latest_files=""

# 1. Companies
local_directory=/opt/airflow/scripts/load/load_db_to_dl
hdfs_directory=/datalake/companies

latest_file=$(ls -t "$local_directory"/*.parquet | head -1 || true)
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in $local_directory"
else
    echo "Uploading $latest_file to HDFS $hdfs_directory ..."
    hdfs dfs -mkdir -p "$hdfs_directory"
    hdfs dfs -put -f "$latest_file" "$hdfs_directory"
    latest_files="${latest_files}${hdfs_directory}/$(basename "$latest_file")\n"
fi

# 2. OHLCS
local_directory=/opt/airflow/scripts/load/load_api_ohlcs_to_dl
hdfs_directory=/datalake/ohlcs

latest_file=$(ls -t "$local_directory"/*.parquet | head -1 || true)
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in $local_directory"
else
    echo "Uploading $latest_file to HDFS $hdfs_directory ..."
    hdfs dfs -mkdir -p "$hdfs_directory"
    hdfs dfs -put -f "$latest_file" "$hdfs_directory"
    latest_files="${latest_files}${hdfs_directory}/$(basename "$latest_file")\n"
fi

# 3. News
local_directory=/opt/airflow/scripts/load/load_api_news_to_dl
hdfs_directory=/datalake/news

latest_file=$(ls -t "$local_directory"/*.parquet | head -1 || true)
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in $local_directory"
else
    echo "Uploading $latest_file to HDFS $hdfs_directory ..."
    hdfs dfs -mkdir -p "$hdfs_directory"
    hdfs dfs -put -f "$latest_file" "$hdfs_directory"
    latest_files="${latest_files}${hdfs_directory}/$(basename "$latest_file")\n"
fi

# Xuất ra danh sách file đã upload (cho Airflow XCom nếu cần)
echo -e "$latest_files"
