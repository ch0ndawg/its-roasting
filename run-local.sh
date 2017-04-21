${SPARK_HOME}/bin/spark-submit \
--master local[6] \
--supervise \
--executor-cores 6 \
/home/ubuntu/its-roasting/target/scala-2.11/it-s-roasting-assembly-0.1.jar 40 100 6 4 0.01
