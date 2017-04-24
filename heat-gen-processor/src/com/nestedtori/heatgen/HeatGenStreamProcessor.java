package com.nestedtori.heatgen;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
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
	public static double C; 
	public static int timeUnit = 100;
	
	public static boolean isBoundary(int i, int j) { 
		return i == 0 || i == numCols - 1 || j ==0 || j == numRows - 1 ;
	}
	
	static boolean isLeftPartitionBoundary(int k) {
		if (k == 0) return false; // actual boundary of the domain doesn't count
		return (k-1)*numPartitions/numCols < k*numPartitions/numCols; 
	}
	
	static boolean isRightPartitionBoundary(int k) {
		if (k == numCols - 1) return false;
		return (k+1)*numPartitions/numCols > k*numPartitions/numCols; 
	}
	
    public static void main(String[] args) throws Exception {
    	Properties props = new Properties();
		numCols  = Integer.parseInt(args[1]);
		numRows = Integer.parseInt(args[2]);

		// default should specify the parameter on the command line
		C = (args.length < 10) ? 0.1875 : Double.parseDouble(args[9]);
      
		HeatGenStreamPartitioner streamPartitioner = new HeatGenStreamPartitioner(numCols);
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "heatgen-processor");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, GridLocationSerde.class);
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, TimeTempTupleSerde.class);
		props.put("num.stream.threads","6");
		
		props.put("key.deserializer", "com.nestedtori.heatgen.serdes.GridLocationDeserializer");
		props.put("value.deserializer", "com.nestedtori.heatgen.serdes.TimeTempTupleDeserializer");
        
        // timestamp conversion
		props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, HeatGenTimestampExtractor.class.getName());
        
        // dummy needed to get # of partitions; streaming doesn't allow ready access to this
		KafkaConsumer<GridLocation,TimeTempTuple> kafkaConsumer = new KafkaConsumer<GridLocation,TimeTempTuple> (props);

		numPartitions = kafkaConsumer.partitionsFor("heatgen-input").size();
		
		kafkaConsumer.close();
        
		if (Integer.parseInt(args[0]) == 0) {
			// if zero, start the producer
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, "heatgen-producer");
			HeatGenProducer hgp = new HeatGenProducer(args);
			(new Thread(hgp )).start();
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
			KTable<GridLocation, TimeTempTuple> pBoundaries = builder.table("partition-boundaries");
	
	         
	        /* KStream<GridLocation, TimeTempTuple> windowedSource = */
			source//.mapValues(v -> new TimeTempTuple ( (long) Math.ceil((double)v.time/timeUnit - 0.5) * timeUnit, v.val ) )
	        	  .aggregateByKey(() -> new TimeTempTuple(0,0.0),
	        			  // sum up multiple occurrences, if necessary
	        			  (k,v,acc) -> new TimeTempTuple( acc.time + 1 /* this is a counter, not a time */, acc.val + v.val),       // future work: use explicit watermarking
	    	       TimeWindows.of("heatgen-windowed", 3*timeUnit /* milliseconds */).advanceBy(timeUnit)) // to account for missing data;  
	        	// change the windowed data into plain timestamp data
	        	  .mapValues( p -> p.time != 0 ? C * p.val/p.time : 0.0 )
	        	  .toStream() // change back to a stream
	        	  .map( (k, v) -> new KeyValue<> (k.key(), new TimeTempTuple(k.window().end(), v))) 
	        	
	        	  .to(streamPartitioner,"heatgen-intermediate-topic"); // repartition the stream
	        // should get average rate in window, and all timestamps should be standardized!
	     
	        // want : table to have (location, uniform timestamp, generation data)
			KTable<GridLocation, TimeTempTuple> windowedSource = builder.table("heatgen-intermediate-topic");
			KTable<GridLocation, TimeTempTuple> inclBoundaries = windowedSource
					.outerJoin( pBoundaries,
	        		(v1, v2) -> {
	        			double gData = (v1 == null? 0.0: v1.val);
	        			if (v2 != null) {
		        			// System.out.println("successfully joined boundary data " + v2 + " to node "+ v1);
	        				gData += C*v2.val; // C is coeff
	        			}
		        		return new TimeTempTuple(v1 != null? v1.time : v2.time, gData);
	        		} /*,
	        		JoinWindows.of("boundary-join") */);
	        // this is because the boundary terms will be guaranteed to belong to the previous time window.
	        
	        // so far: each gridLocation should contain either
	        // a list singleton of heat generation data, and in the boundary case,
	        // both the singleton and the previous result
	        
			KStream<GridLocation, TimeTempTuple> newTemp = inclBoundaries.toStream()
	    	// the most important part:
	    	// a custom transformer that retrieves current values in a state store,
	    	// computes the stencil (4 nearest neighbors), multiplied by the appropriate constant
					.transform(
	    			 () -> new CurrentTempTransformer() 
	    		, "current")
					.transform(() -> new NewTempSaver(numRows, numCols), "current");
	
	    	// as of now, newTemp should contain the new temperature values indexed by key
	    	
			KStream<GridLocation,TimeTempTuple>[] partitionBoundaryStreams =  
					newTemp.through(streamPartitioner,"temp-output")
	    	//.transform(() -> new SaveToCassandraTransformer("52.10.235.41",
	    	//		leftX, rightX, bottomY, topY)) 
					.branch((k,v) -> isLeftPartitionBoundary(k.i),
							(k,v) -> isRightPartitionBoundary(k.i));
	    
	        
	    	
			partitionBoundaryStreams[0].map((k,v)->new KeyValue<>(new GridLocation(k.i - 1, k.j), new TimeTempTuple(v.time + timeUnit, v.val)))
				.to(streamPartitioner, "partition-boundaries"); 
			partitionBoundaryStreams[1].map((k,v)->new KeyValue<>(new GridLocation(k.i + 1, k.j), new TimeTempTuple(v.time + timeUnit, v.val)))
				.to(streamPartitioner, "partition-boundaries"); 
			
			KafkaStreams streams = new KafkaStreams(builder, props);
			streams.start();
	
			Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		}
    }
}
