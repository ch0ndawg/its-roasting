package com.nestedtori.heatgen;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.state.Stores;

import com.nestedtori.heatgen.datatypes.*;
import com.nestedtori.heatgen.serdes.*;
import scala.Tuple2;

import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.lang.Thread;

public class HeatGenStreamProcessor {
	public static int numCols;
	public static int numRows;
	public static int numPartitions;
	public static int numInEach;
	public static double C; 
	
	public static boolean isBoundary(int i, int j) { 
		return i == 0 || i == numCols - 1 || j ==0 || j == numRows - 1 ;
	}
	
	public static boolean isLeftPartitionBoundary(int k) {
		if (k == 0) return false; // actual boundary of the domain doesn't count
		return k % numInEach == 0;
	}
	
	static boolean isRightPartitionBoundary(int k) {
		if (k == numCols - 1) return false;
		return k % numInEach == numInEach - 1;
	}
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
		numCols  = Integer.parseInt(args[1]);
		numRows = Integer.parseInt(args[2]);
		// default should specify the parameter on the command line
		 C = (args.length < 10) ? 0.1875 : Double.parseDouble(args[9]);
      
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
        
        // dummy needed to get # of partitions. I don't know why streaming doesn't allow ready access to this
        KafkaConsumer<GridLocation,TimeTempTuple> kafkaConsumer = new KafkaConsumer<GridLocation,TimeTempTuple> (props);

        kafkaConsumer.close();

        numPartitions = kafkaConsumer.partitionsFor("heatgen-input").size();
		numInEach = (int) (Math.ceil(numCols / (double)numPartitions) + 0.1);
		
		if (Integer.parseInt(args[0]) == 0) { // if zero, start the producer
			HeatGenProducer hgp = new HeatGenProducer(args);
			(new Thread(hgp)).start();
			
		} else { // run the streamer app
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
	        KStream<GridLocation, TimeTempTuple> pBoundaries = builder.stream("partition-boundaries");
	
	        
	        KStream<GridLocation, TimeTempTuple> windowedSource = source
	        	.aggregateByKey(() -> new Tuple2<Double,Integer>(0.0,0),
	        			// sum up multiple occurrences, if necessary
	        	(k,v,acc) -> new Tuple2<Double,Integer>(acc._1() + v.val, acc._2() + 1),
	        	TimeWindows.of("heatgen-windowed", 100 /* milliseconds */)) // could change this to hopping for better data
	        	// change the windowed data into plain timestamp data
	        	.toStream() // change back to a stream
	        	.map( (k,p) -> new KeyValue<GridLocation,TimeTempTuple>(k.key(),
	        	     new TimeTempTuple(k.window().end(), p._2() != 0 ? C * p._1()/p._2() : 0.0 ))
	        	); // should get average rate in window
	     
	        // want : table to have (location, uniform timestamp, generation data)
	        
	        KStream<GridLocation, List<TimeTempTuple>> inclBoundaries = windowedSource
	        		.outerJoin( pBoundaries,
	        		 (v1, v2) -> {
	        		List<TimeTempTuple> result = new ArrayList<TimeTempTuple>(); // empty 
	        		if (v1 != null) {
	        			result.add(v1);
	        		}
	        		if (v2 != null) {
	        			result.add(new TimeTempTuple(v2.time,C*v2.val)); // C is coeff
	        		}
	        		return result;
	        	},
	        JoinWindows.of("boundary-join").before(100 /* milliseconds */));
	        
	        // so far: each gridLocation should contain either
	        // a list singleton of heat generation data, and in the boundary case,
	        // both the singleton and the previous result
	        
	    	KStream<GridLocation, TimeTempTuple> newTemp = inclBoundaries.transform(
	    			 () -> new CurrentTempTransformer() // the most important part!
	    		, "current") // custom transformer that retrieves state store and multiplies by constant
	    	.flatMapValues(value -> value) // a.k.a concatenate; it's already a list!
	    	.reduceByKey((a,b) -> new TimeTempTuple(Math.max(a.time, b.time), a.val+b.val),
	    			TimeWindows.of("Reductions", 100 /* milliseconds */))  // sum up the stencil and generation data
	    	.toStream().map ( (k,p) -> new KeyValue<>(k.key(),p)) // remove the window key
	    	.transform ( ()-> new NewTempSaver(), "current"); // write back to state store 
	
	    	// as of now, newTemp should contain the new temperature values indexed by key
	    	
	    	KStream<GridLocation,TimeTempTuple>[] partitionBoundaryStreams =  
	        newTemp.through("temp-output").branch((k,v) -> isLeftPartitionBoundary(k.i),
	        		(k,v) -> isRightPartitionBoundary(k.i));
	        
	    	
	        partitionBoundaryStreams[0].map((k,v)->new KeyValue<>(new GridLocation(k.i - 1, k.j), v))
	        		.to(streamPartitioner, "partition-boundaries"); 
	        partitionBoundaryStreams[1].map((k,v)->new KeyValue<>(new GridLocation(k.i + 1, k.j), v))
	        		.to(streamPartitioner, "partition-boundaries"); 
	        
	        KafkaStreams streams = new KafkaStreams(builder, props);
	        streams.start();
	
	        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		}
    }
}
