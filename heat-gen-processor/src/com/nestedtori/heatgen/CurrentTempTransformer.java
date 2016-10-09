package com.nestedtori.heatgen;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;

import com.nestedtori.heatgen.datatypes.GridLocation;
import com.nestedtori.heatgen.datatypes.TimeTempTuple;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.processor.ProcessorContext;

public class CurrentTempTransformer implements Transformer<GridLocation,TimeTempTuple,KeyValue<GridLocation,TimeTempTuple>> {
		private KeyValueStore<GridLocation, Double> kvStore;
		
		@Override
		@SuppressWarnings("unchecked")
		public void init(ProcessorContext context) {
			kvStore = (KeyValueStore<GridLocation, Double>) context.getStateStore("current");
		}
		
		@Override
		public KeyValue<GridLocation,TimeTempTuple> transform(GridLocation key, TimeTempTuple value) {
			// KeyValue<GridLocation,List<TimeTempTuple>> result = new KeyValue<>(key, new ArrayList<TimeTempTuple>());
			
			double result = value.val;
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
				result += thisValue - 4 * HeatGenStreamProcessor.C * thisValue;			
			if (above != null)
				result += HeatGenStreamProcessor.C* above;
			if (right != null)
				result += HeatGenStreamProcessor.C* right;
			if (below != null)
				result += HeatGenStreamProcessor.C* below;
			if (left != null)
				result += HeatGenStreamProcessor.C* left;		
			
			return new KeyValue<>(key, new TimeTempTuple(value.time, result));
		}
		
		@Override
		public KeyValue<GridLocation,TimeTempTuple > punctuate(long timestamp) {
			return new KeyValue<>(new GridLocation(0,0), new TimeTempTuple(0,0.0)); // empty; we do no periodic work
		}
		@Override
		public void close() {}
}
