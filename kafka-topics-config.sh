# Create heat generation topic with 6 partitions

kafka-topics.sh --create \
            --zookeeper localhost:2181 \
            --replication-factor 3 \
            --partitions 6 \
            --topic heatgen-input
            
# kafka-topics.sh --zookeeper localhost:2181 --alter --topic heatgen-input 
#       --partitions 18 
