package com.nestedtori.heatgen;

import org.apache.kafka.streams.processor.AbstractProcessor;
import com.nestedtori.heatgen.datatypes.GridLocation;
import com.nestedtori.heatgen.datatypes.TimeTempTuple;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.processor.ProcessorContext;


public class NewTempSaver extends AbstractProcessor<GridLocation,TimeTempTuple> {
	private KeyValueStore<GridLocation, TimeTempTuple> kvStore;
	
	@Override
	@SuppressWarnings("unchecked")
	public void init(ProcessorContext context) {
		super.init(context);
		kvStore = (KeyValueStore<GridLocation, TimeTempTuple>) context.getStateStore("current");
	}
	
	@Override
	public void process(GridLocation key, TimeTempTuple value) {
		context().forward(key, value); // forward k,v as is
	}
	
}
