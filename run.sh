spark-submit \
--master spark://ip-172-31-0-74:6066 \
--deploy-mode cluster
--supervise \
--executor-cores 6 \
/home/ubuntu/its-roasting/target/scala-2.10/it-s-roasting-assembly.jar 