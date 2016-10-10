package com.nestedtori.heatgen;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
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

		double leftX = (args.length < 6)? -10.0 : Double.parseDouble(args[5]);
		double rightX = (args.length < 7)? 10.0 : Double.parseDouble(args[6]);
		double bottomY =(args.length < 8)? -10.0 : Double.parseDouble(args[7]);
		double topY = (args.length < 9)? 10.0 : Double.parseDouble(args[8]);
		
		// default should specify the parameter on the command line
		 C = (args.length < 10) ? 0.1875 : Double.parseDouble(args[9]);
      
		HeatGenStreamPartitioner streamPartitioner = new HeatGenStreamPartitioner(numCols);
        
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "heatgen-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, GridLocationSerde.class);
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, TimeTempTupleSerde.class);
        
        props.put("key.deserializer", "com.nestedtori.heatgen.serdes.GridLocationDeserializer");
        props.put("value.deserializer", "com.nestedtori.heatgen.serdes.TimeTempTupleDeserializer");
        
        // timestamp conversion
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, HeatGenTimestampExtractor.class.getName());
        
        // dummy needed to get # of partitions. I don't know why streaming doesn't allow ready access to this
        KafkaConsumer<GridLocation,TimeTempTuple> kafkaConsumer = new KafkaConsumer<GridLocation,TimeTempTuple> (props);

        numPartitions = kafkaConsumer.partitionsFor("heatgen-input").size();
		numInEach = (int) (Math.ceil(numCols / (double)numPartitions) + 0.1);
		
        kafkaConsumer.close();
        
		if (Integer.parseInt(args[0]) == 0) { // if zero, start the producer
	        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "heatgen-producer");
			HeatGenProducer hgp = new HeatGenProducer(args);
			(new Thread(hgp)).start();
			
		} else if (Integer.parseInt(args[0]) == 2) {
			 // run the consumer
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, "temp-consumer");
			TempConsumer tc = new TempConsumer(args);
			(new Thread(tc)).start();
		} else {
			// run the streamer
	        // make sure heatgen-input is copartitioned with partition-boundaries
	        // consider using another topic as a state store
	        
	        StateStoreSupplier currentTemp = Stores.create("current")
	        		                               .withKeys(new GridLocationSerde())
	        		                               .withValues(Serdes.Double())
	        		                               .inMemory()
	        		                               .build();
	        
	        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
	        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	
	        KStreamBuilder builder = new KStreamBuilder();
	        
	        builder.addStateStore(currentTemp);
	        
	        KStream<GridLocation, TimeTempTuple> source = builder.stream("heatgen-input");
	        KStream<GridLocation, TimeTempTuple> pBoundaries = builder.stream("partition-boundaries");
	
	        
	        KStream<GridLocation, TimeTempTuple> windowedSource = source
	        	.aggregateByKey(() -> new TimeTempTuple(0,0.0),
	        			// sum up multiple occurrences, if necessary
	        	(k,v,acc) -> new TimeTempTuple( acc.time + 1 , acc.val + v.val),       // future work: use explicit watermarking
	        	TimeWindows.of("heatgen-windowed", 300 /* milliseconds */).advanceBy(100).until(1000)) // to account for missing data;  
	        	// change the windowed data into plain timestamp data
	        	.toStream() // change back to a stream
	        	.map( (k,p) -> new KeyValue<GridLocation,TimeTempTuple>(k.key(),
	        	     new TimeTempTuple(k.window().end(), p.time != 0 ? C * p.val/p.time : 0.0 ))
	        	).through(streamPartitioner,"heatgen-intermediate-topic"); // repartition the stream
	        // should get average rate in window, and all timestamps should be standardized!
	     
	        // want : table to have (location, uniform timestamp, generation data)
	        
	        KStream<GridLocation, TimeTempTuple> inclBoundaries = windowedSource
	        	.leftJoin( pBoundaries,
	        		(v1, v2) -> {
		        		double gData = v1.val;
		        		if (v2 != null) {
		        			gData += C*v2.val; // C is coeff
		        		}
		        		return new TimeTempTuple(v1.time, gData);
		        	},
	        		JoinWindows.of("boundary-join").before(200 /* milliseconds */));
	        // this is because the boundary terms will be guaranteed to belong to the previous time window.
	        
	        // so far: each gridLocation should contain either
	        // a list singleton of heat generation data, and in the boundary case,
	        // both the singleton and the previous result
	        
	    	KStream<GridLocation, TimeTempTuple> newTemp = inclBoundaries
	    	// the most important part:
	    	// a custom transformer that retrieves current values in a state store,
	    	// computes the stencil (4 nearest neighbors), multiplied by the appropriate constant
	    	.transform(
	    			 () -> new CurrentTempTransformer() 
	    		, "current")
	    	.transform(() -> new NewTempSaver(), "current");
	
	    	// as of now, newTemp should contain the new temperature values indexed by key
	    	
	    	KStream<GridLocation,TimeTempTuple>[] partitionBoundaryStreams =  
	        newTemp.through(streamPartitioner,"temp-output")
	    	//.transform(() -> new SaveToCassandraTransformer("52.10.235.41",
	    	//		leftX, rightX, bottomY, topY)) 
	        .branch((k,v) -> isLeftPartitionBoundary(k.i),
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
