package com.nestedtori.heatgen.serdes;

import com.nestedtori.heatgen.datatypes.GridLocation;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import java.util.Map;

public class GridLocationDeserializer implements Deserializer<GridLocation> {
	private LongDeserializer longDeserializer; 
	
	public GridLocationDeserializer() {
		longDeserializer = new LongDeserializer();
	}
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	public GridLocation deserialize(String topic, byte[] data) {
		long packed = longDeserializer.deserialize(topic, data);
		GridLocation result = new GridLocation(0,0);
		result.i = (int) (packed >>> 32);
		result.j = (int) (packed & 0x00000000FFFFFFFF); // this might be unnecessary
		return result;
	}
	
	public void close() {
		// do nothing
	}
}
