package com.nestedtori.heatgen;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;

import com.nestedtori.heatgen.datatypes.GridLocation;
import com.nestedtori.heatgen.datatypes.TimeTempTuple;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.processor.ProcessorContext;


public class NewTempSaver implements Transformer<GridLocation,TimeTempTuple, KeyValue<GridLocation,TimeTempTuple>> {
	private KeyValueStore<GridLocation, Double> kvStore;
	
	@Override
	@SuppressWarnings("unchecked")
	public void init(ProcessorContext context) {
		kvStore = (KeyValueStore<GridLocation, Double>) context.getStateStore("current");
	}
	
	@Override
	public KeyValue<GridLocation,TimeTempTuple> transform(GridLocation key, TimeTempTuple value) {
		kvStore.put(key, value.val);
		return new KeyValue<>(key, value); // return k,v as is
	}
	
	public void close() {}
	
	public KeyValue<GridLocation,TimeTempTuple> punctuate(long timestamp) {
		return new KeyValue<>(new GridLocation(0,0), new TimeTempTuple(0,0.0));
	}
	
}
