package com.nestedtori.heatgen;

import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import com.nestedtori.heatgen.datatypes.GridLocation;

public class HeatGenPartitioner implements Partitioner {
	int n;
	public void configure(Map<String, ?> configs) {
		
	}
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster)
	{
		n = cluster.partitionCountForTopic("heatgen-input");
		GridLocation pair = (GridLocation) key;
		return pair.i*n/HeatGenProducer.numCols; // partition into slices. assumes zero based indexing
	}
	
	public void close() { }
}
