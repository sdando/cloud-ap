hadoop fs -rmr CKP_DIR
../bin/spark-submit --master spark://192.168.10.222:7077 --class ap.v3.APCluster ap.jar hdfs://192.168.10.222:9000/user/zx/output/temp/SAR/part* result 200 10 100 0.9 -0.19 2
