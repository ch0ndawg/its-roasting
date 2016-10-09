package com.nestedtori.heatgen.serdes;

import com.nestedtori.heatgen.datatypes.*;
import java.util.Map;

import org.apache.kafka.common.serialization.Serde;

public class TimeTempTupleSerde implements Serde<TimeTempTuple> {
	private TimeTempTupleSerializer gls;
	private TimeTempTupleDeserializer glds;
	
	public TimeTempTupleSerde() {
		gls = new TimeTempTupleSerializer();
		glds = new TimeTempTupleDeserializer();
	}
	public void configure(Map<String, ?> configs, boolean isKey) {
	
	}
	
	public TimeTempTupleSerializer serializer() { return gls; }
	public TimeTempTupleDeserializer deserializer() { return glds; }
	
	public void close() {}
}
