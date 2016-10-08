kafka-console-consumer.sh --zookeeper localhost:2181 \
    --topic heatgen-input             \
    --from-beginning         \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true          \
    --property print.value=true          \
    --property key.deserializer=com.nestedtori.heatgen.serdes.GridLocationDeserializer  \
    --property value.deserializer=com.nestedtori.heatgen.serdes.TimeTempTupleDeserializer

# args:
# <which app> <x grid size> <y grid size> <rate param> <probability param>
# <left X default = -10> <right X default = 10> <bottom Y default = -10 > <top Y default=10>
# <conductivity param default = 0.1875 >

java -jar target/heat-gen-processor-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
0 40 40 4.0 0.01 &

# 40 x 40 grid on [-10,10] x [-10,10], rate is 4.0 calories per unit area per unit time

# process
java -jar target/heat-gen-processor-0.0.1-SNAPSHOT-jar-with-dependencies.jar  \
1 40 40  &
