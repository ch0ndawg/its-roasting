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
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.state.Stores;

import com.nestedtori.heatgen.datatypes.*;
import com.nestedtori.heatgen.serdes.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Properties;

public class HeatGenStreamProcessor {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
		final int numCols = Integer.parseInt(args[0]);
      
		HeatGenStreamPartitioner streamPartitioner = new HeatGenStreamPartitioner(numCols);
        Serde<GridLocation> S = Serdes.serdeFrom(new GridLocationSerializer(), new GridLocationDeserializer());
        Serde<TimeTempTuple> GS = Serdes.serdeFrom(new TimeTempTupleSerializer(), new TimeTempTupleDeserializer());
      
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "heatgen-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, S.getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GS.getClass().getName());
        
        // timestamp conversion
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, HeatGenTimestampExtractor.class.getName());
        
        // make sure heatgen-input is copartitioned with partition-boundaries
        // consider using another topic as a state store
        
        StateStoreSupplier currentTemp = Stores.create("current")
        		                               .withKeys(S.getClass())
        		                               .withValues(Serdes.Double())
        		                               .persistent()
        		                               .build();
        
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();
        
        builder.addStateStore(currentTemp);
        
        KStream<GridLocation, TimeTempTuple> source = builder.stream("heatgen-input");
        KTable<GridLocation, TimeTempTuple> pBoundaries = builder.table("partition-boundaries");

        
        KTable<GridLocation, TimeTempTuple> windowedSource = source
        	.aggregateByKey(() -> new Tuple2<Double,Int>(0.0,0) , (k,v,acc) -> new Tuple2<Double,Int>(acc.val + v.heatgen, acc.count + 1),
        	TimeWindows.of("heatgen-windowed", 100 /* milliseconds */)) // could change this to hopping for better data
        	.filter((k,p) -> (p._2() != 0)) // average totaling over window
        	// change the windowed data into plain timestamp data
        	.map( (k,p) -> new KeyValue<>(k.key(),
        	     new TimeTempTuple(k.window().end(), C * p.val/p.count))
        	); // should get average rate in window
       // want : table to have (location, uniform timestamp, generation data)
        
        KStream<GridLocation, List<TimeTempTuple>> inclBoundaries = windowedSource.outerJoin(pBoundaries, (v1,v2) -> {
        	List<TimeTempTuple> result = new ArrayList<TimeTempTuple>(); // empty 
        	if (v1 != null) {
        		result.add(v1);
        	}
        	if (v2 != null) {
        		result.add(new TimeTempTuple(v2.time,C*v2.val)); // C is coeff
        	}
        	return result;
        }).toStream();
        
        // so far: each gridLocation should contain either
        // a list singleton of heat generation data, and in the boundary case,
        // both the singleton and the previous result
        
        // 
    	inclBoundaries.transform(
    			// ok I REALLY should rewrite this in Scala
    		new TransformerSupplier<GridLocation,List<TimeTempTuple>,
    		KeyValue<GridLocation,List<TimeTempTuple>>>() {
    			@SuppressWarnings("unchecked")
    			public Transformer<GridLocation,List<TimeTempTuple>,
    			KeyValue<GridLocation,List<TimeTempTuple> >> get()
    			{
    				return new CurrentTempTransformer();
    			}
    		}, "current") // custom processor that retrieves state store and multiplies by constant
    	.flatMapValues((List<TimeTempTuple> value) -> value) // a.k.a concatenate; it's already a list!
    	.reduceByKey((a,b) -> new TimeTempTuple(Math.max(a.time, b.time), a.val+b.val))  // sum up the stencil and generation data
    	.process (() -> new Processor() {
    		private KeyValueStore<GridLocation, TimeTempTuple> kvStore;
			
			@Override
			@SuppressWarnings("unchecked")
			public init(ProcessorContext context) {
				kvStore = (KeyValueStore) context.getStateStore("current");
			}
    		@Override
			public void process(GridLocation key, List<TimeTempTuple> value) {
    			
    		}
    	}, "current") // write back to state store 

        .through("temp-output").filter((k,v) -> isBoundary(k))
        //.map((pb,x) -> (pb + or - 1,x))
        .to(streamPartitioner, "partition-boundaries"); // may not even need to write to any other topic than partition boundaries
        
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(KafkaStreams::close));
    }
}
