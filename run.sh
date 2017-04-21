${SPARK_HOME}/bin/spark-submit \
--master spark://ip-172-31-44-232:7077 \
--supervise \
--executor-cores 6 \
/home/ubuntu/its-roasting/target/scala-2.11/it-s-roasting-assembly-0.1.jar 40 400 6 4 0.01
