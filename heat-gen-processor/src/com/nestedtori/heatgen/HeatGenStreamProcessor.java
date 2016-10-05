package com.nestedtori.heatgen;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import com.nestedtori.heatgen.datatypes.*;
import com.nestedtori.heatgen.serdes.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public class HeatGenStreamProcessor {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        Serde<GridLocation> S = Serdes.serdeFrom(new GridLocationSerializer(), new GridLocationDeserializer());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "heatgen-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, S.getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        
        // timestamp conversion
        
        // make sure heatgen-input is copartitioned with partition-boundaries
        // consider using another topic as a state store
        
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();
        
        KStream<GridLocation, GenDataTime> source = builder.stream("heatgen-input");
        KTable<GridLocation, GenDataTime> pBoundaries = builder.table("partition-boundaries");

        
        KTable<GridLocation, GenDataTime> windowedSource = source
        	.aggregateByKey(()-> new Tuple2<Double,Int>(0.0,0) , (k,v,acc) -> new Tuple2<Double,Int>(acc.val + v.heatgen, acc.count + 1),
        	TimeWindows.of("heatgen-windowed", 100 /* milliseconds */)) // could change this to hopping for better data
        	.filter((k,p) -> (p._2() != 0)) // average totaling over window
        	// change the windowed data into plain timestamp data
        	.map( (k,p) -> new KeyValue<>(k.key(),
        	     new GenDataTime(k.window().end(), C * p.val/p.count))
        	); // should get average rate in window
       // want : table to have (location, uniform timestamp, generation data)
        
        KStream newData = windowedSource.outerJoin(pBoundaries, (v1,v2) -> {
        	List<TempValTuple> result = new List<TempValTuple>(); // empty 
        	if (v1 != null) {
        		result.add(v1);
        	}
        	if (v2 != null) {
        		result.add(v2);
        	}
        	return result;
        }).toStream()
        	.process(()->new Processor() {
        				public init() {
        					
        				}
        				public void process(key, value) {
        					
        				}
        			}) // custom processor that retrieves state store and multiplies by constant
        	.flatMapValues ()// into list, changing nulls to empty list
        	.reduceByKey(_+_)  // sum up the stencil and generation data
        	.process (); // write back to state store 

        newData.through("temp-output").filter((k,v) -> isBoundary(k))
        .map((pb,x) -> (pb + or - 1,x))
        .to("partition-boundaries"); // may not even need to write to any other topic than partition boundaries
        
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(KafkaStreams::close));
    }
}
