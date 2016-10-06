package com.nestedtori.heatgen;

import org.apache.kafka.clients.producer.ProducerConfig;
import com.nestedtori.heatgen.datatypes.GridLocation;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;


public class HeatGenProducer {
	public static int numCols;
	// singleton ; number of columns is solely for the purpose of partitioning
	// N is not necessarily consistent with the actual number as given in the data itself
	//HeatGenProducer(int N) { numCols = N; }
	public static void main(String[] args) {
		numCols = Integer.parseInt(args[0]);
		Properties props = new Properties();
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, HeatGenPartitioner.class.getName());
	}
}
