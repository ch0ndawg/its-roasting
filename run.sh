${SPARK_HOME}/bin/spark-submit \
--master spark://ip-172-31-0-77:6066 \
--supervise \
--executor-cores 6 \
/home/ubuntu/its-roasting/target/scala-2.10/it-s-roasting-assembly-0.1.jar 40 5000 6 4 0.01
