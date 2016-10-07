package com.nestedtori.heatgen;

import java.util.List;
import java.util.ArrayList;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;

import com.nestedtori.heatgen.datatypes.GridLocation;
import com.nestedtori.heatgen.datatypes.TimeTempTuple;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.processor.ProcessorContext;

public class CurrentTempTransformer implements Transformer<GridLocation,List<TimeTempTuple>,KeyValue<GridLocation,List<TimeTempTuple> >> {
		private KeyValueStore<GridLocation, TimeTempTuple> kvStore;
		
		@Override
		@SuppressWarnings("unchecked")
		public void init(ProcessorContext context) {
			kvStore = (KeyValueStore<GridLocation, TimeTempTuple>) context.getStateStore("current");
		}
		
		@Override
		public KeyValue<GridLocation,List<TimeTempTuple> > transform(GridLocation key, List<TimeTempTuple> value) {
			KeyValue<GridLocation,List<TimeTempTuple>> result = new KeyValue<>(key, new ArrayList<TimeTempTuple>());
			return result;
		}
		
		@Override
		public KeyValue<GridLocation,List<TimeTempTuple> > punctuate(long timestamp) {
			return new KeyValue<>(new GridLocation(0,0), new ArrayList<TimeTempTuple>()); // empty; we do no periodic work
		}
		@Override
		public void close() {}
}
