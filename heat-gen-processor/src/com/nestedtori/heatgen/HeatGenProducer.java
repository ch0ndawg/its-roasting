package com.nestedtori.heatgen;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import com.nestedtori.heatgen.datatypes.*;
import com.nestedtori.heatgen.serdes.GridLocationDeserializer;
import com.nestedtori.heatgen.serdes.GridLocationSerializer;
import com.nestedtori.heatgen.serdes.TimeTempTupleDeserializer;
import com.nestedtori.heatgen.serdes.TimeTempTupleSerializer;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Random;

public class HeatGenProducer {
	public static int numCols;
	public static int numRows;
	// singleton ; number of columns is solely for the purpose of partitioning
	// N is not necessarily consistent with the actual number as given in the data itself
	//HeatGenProducer(int N) { numCols = N; }
	public static void main(String[] args) {
		Properties props = new Properties();
		
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, HeatGenPartitioner.class.getName());
        Serde<GridLocation> S = Serdes.serdeFrom(new GridLocationSerializer(), new GridLocationDeserializer());
        Serde<TimeTempTuple> GS = Serdes.serdeFrom(new TimeTempTupleSerializer(), new TimeTempTupleDeserializer());
      
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "heatgen-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, S.getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GS.getClass().getName());
        props.put("buffer.memory", 33554432);
        
		KafkaProducer<GridLocation, TimeTempTuple> producer = 
				new KafkaProducer<GridLocation, TimeTempTuple>(props);
		
		Random rng = new Random();
		
		numCols = Integer.parseInt(args[0]);
		numRows = Integer.parseInt(args[1]);
		double rateParam = Double.parseDouble(args[2]);
		double probParam = Double.parseDouble(args[3]);
		double leftX = (args.length >= 5)? -10.0 : Double.parseDouble(args[4]);
		double rightX = (args.length >= 6)? 10.0 : Double.parseDouble(args[5]);
		double bottomY =(args.length >= 7)? -10.0 : Double.parseDouble(args[6]);
		double topY = (args.length >= 8)? 10.0 : Double.parseDouble(args[7]);
		double sigma = 3.0;
		double C = 0.1875;
		
		double dx  = (rightX - leftX)/(numCols-1);
		double dy = (topY - bottomY)/(numCols-1);
		
		int timeUnit = 100;
		double tolerance = 0.1;
		double meanError = timeUnit*tolerance;

		Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
		
		try {
			for (int iter = timeUnit; ; iter+=timeUnit) {
				for (int i=0 ; i < numCols; i++) {
					for (int j=0; j < numRows; j++ ) {
						double dtf =  (rng.nextDouble() < probParam) ? dx*dy *rateParam : 0.0; // streaming data at timestep; coeff is k dt/dx^2
						double increment = rng.nextGaussian()*meanError;
						long timestamp = Math.round(increment) + iter;
						producer.send(new ProducerRecord<>("heatgen-input", new GridLocation(i,j),
								new TimeTempTuple(timestamp,dtf)));
						Thread.sleep(timeUnit);
					}
				}
				
			}
		} catch (InterruptedException ie) { }
		finally { producer.close(); }
	}
}
