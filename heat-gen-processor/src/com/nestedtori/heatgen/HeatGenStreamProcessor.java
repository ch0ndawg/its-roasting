package com.nestedtori.heatgen;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public class HeatGenStreamProcessor {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "heatgen-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        
        // timestamp conversion
        
        // make sure heatgen-input is copartitioned with partition-boundaries
        // consider using another topic as a state store
        
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();       
        
        KStream<GridLocation, GenDataTime> source = builder.stream("heatgen-input");
        KTable<GridLocation, GenDataTime> pBoundaries = builder.table("partition-boundaries");

       /* KTable<String, Long> counts = source
                .flatMapValues(val ->	Arrays.asList(val.toLowerCase(Locale.getDefault()).split(" ")))	
                .map( (key,val) -> new KeyValue<>(val, val))
                .countByKey( "Count" ); */
        
        KTable<GridLocation, TempValTuple> windowedSource = source
        	.aggregateByKey((0,0) , (k,v,acc) -> (acc.val + v.heatgen, acc.count + 1),
        	TimeWindows.of("heatgen-windowed", 100 /* milliseconds */))
        	.filter((k,p) -> (p!= (0,0))
        	// change the windowed data into plain timestamp data
        	.map( (k,p) -> (k.key(), k.window().end(), C * p.val/p.count)) ; // should get average rate in window
       // want : table to have (location, uniform timestamp, generation data)
        
        KStream newData = windowedSource.outerJoin(pBoundaries, (v1,v2) -> {
        	List<TempValTuple>
        	if (v1 != null && v2 != null) {
        		
        	} else if (v1 == null) {
        
        	} else if (v2 == null) {
        		
        	} else {
        		return 
        	}
        })
        	.process( ) // custom processor that retrieves state store and multiplies by constant
        	.flatMapValues ()// into list, changing nulls to empty list
        	.reduceByKey(_+_)  // sum up the stencil and generation data
        	.process () // write back to state store 
        	.toStream();

        newData.through("temp-output").filter((k,v) -> isBoundary(k)).to("partition-boundaries");
        
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        /* try {
	        while (true) {
	        // usually the stream application would be running forever,
	        // in this example we just let it run for some time and stop since the input data is finite.
	          Thread.sleep(5000L);
	        }
        } catch (InterruptedException ie) {}
        streams.close(); // */
        Runtime.getRuntime().addShutdownHook(new Thread(KafkaStreams::close));
    }
}
