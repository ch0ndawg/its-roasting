package com.nestedtori.heatgen;

import org.apache.kafka.streams.processor.StreamPartitioner;
import com.nestedtori.heatgen.datatypes.*;


public class HeatGenStreamPartitioner implements StreamPartitioner<GridLocation,TimeTempTuple> {

	int numCols;
	HeatGenStreamPartitioner(int N) { numCols = N; }	
	@Override
	public Integer partition(GridLocation key,
			TimeTempTuple value,
            int numPartitions) {
		return key.i*numPartitions/numCols;
	}

}
