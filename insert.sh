THREADS=$1
INSERTS=$2
HOSTS_CSV=$3
NAMESPACE=$4
SET=$5
START_KEY=$6
java -jar out/artifacts/aerospike_threads_async_jar/aerospike_threads_async.jar $THREADS $INSERTS $HOSTS_CSV $NAMESPACE $SET $START_KEY
