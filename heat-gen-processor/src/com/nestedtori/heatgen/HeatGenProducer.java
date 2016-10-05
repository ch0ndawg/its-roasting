package com.nestedtori.heatgen;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.Serdes;
import scala.Tuple2;

class HeatGenPartitioner implements Partitioner {
	//private final List<PartitionInfo> partitionData = partitionsFor("heatgen-input");
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster)
	{
		final int n = cluster.partitionCountForTopic("heatgen-input");
		Tuple2<Integer,Integer> pair = (Tuple2<Integer,Integer>) key;
		return pair._1()*n/HeatGenProducer.numCols; // partition into slices. assumes zero based indexing
	}
}

public class HeatGenProducer {
	static int numCols;
	// singleton ; number of columns is solely for the purpose of partitioning
	// N is not necessarily consistent with the actual number as given in the data itself
	//HeatGenProducer(int N) { numCols = N; }
	public static void main(String[] args) {
		numCols = Integer.parseInt(args[0]);
		Properties props = new Properties();
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, HeatGenPartitioner.class.getName());
	}
}
