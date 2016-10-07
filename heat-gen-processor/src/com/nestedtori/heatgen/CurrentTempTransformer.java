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
		private KeyValueStore<GridLocation, Double> kvStore;
		
		@Override
		@SuppressWarnings("unchecked")
		public void init(ProcessorContext context) {
			kvStore = (KeyValueStore<GridLocation, Double>) context.getStateStore("current");
		}
		
		@Override
		public KeyValue<GridLocation,List<TimeTempTuple> > transform(GridLocation key, List<TimeTempTuple> value) {
			// KeyValue<GridLocation,List<TimeTempTuple>> result = new KeyValue<>(key, new ArrayList<TimeTempTuple>());
			
			TimeTempTuple first = value.get(0);
			// the stencil
			// note that this temp value is value at the other points,
			// unlike the batch version which spreads this value to the other indices
			// it all comes out in the wash (erm. Reduction)
			Double thisValue = kvStore.get(key);
			Double above = kvStore.get(new GridLocation(key.i,key.j+1) );
			Double right = kvStore.get(new GridLocation(key.i+1,key.j) );
			Double below = kvStore.get(new GridLocation(key.i,key.j-1) );
			Double left = kvStore.get(new GridLocation(key.i-1,key.j) );
			
			// nulls are treated as ZERO
			// This has the effect of enforcing Dirichlet boundary conditions on cells that are
			// ONE PAST the edges (in effect, they are ghost cells), UNLIKE the batch version
			if (thisValue != null)
				value.add(new TimeTempTuple(first.time,-4 * HeatGenStreamProcessor.C* thisValue));			
			if (above != null)
				value.add(new TimeTempTuple(first.time, HeatGenStreamProcessor.C* above));
			if (right != null)
					value.add(new TimeTempTuple(first.time, HeatGenStreamProcessor.C* right));
			if (below != null)
				value.add(new TimeTempTuple(first.time, HeatGenStreamProcessor.C* below));
			if (left != null)
				value.add(new TimeTempTuple(first.time, HeatGenStreamProcessor.C* left));			
			
			return new KeyValue<>(key, value);
		}
		
		@Override
		public KeyValue<GridLocation,List<TimeTempTuple> > punctuate(long timestamp) {
			return new KeyValue<>(new GridLocation(0,0), new ArrayList<TimeTempTuple>()); // empty; we do no periodic work
		}
		@Override
		public void close() {}
}
