package com.nestedtori.heatgen.serdes;

import org.apache.kafka.common.serialization.Serde;
import com.nestedtori.heatgen.datatypes.GridLocation;
import java.util.Map;

public class GridLocationSerde implements Serde<GridLocation> {
	private GridLocationSerializer gls;
	private GridLocationDeserializer glds;
	
	public GridLocationSerde() {
		gls = new GridLocationSerializer();
		glds = new GridLocationDeserializer();
	}
	
	public void configure(Map<String, ?> configs, boolean isKey) {

	}
	
	public GridLocationSerializer serializer() { return gls; }
	public GridLocationDeserializer deserializer() { return glds; }
	
	public void close() {}
}
