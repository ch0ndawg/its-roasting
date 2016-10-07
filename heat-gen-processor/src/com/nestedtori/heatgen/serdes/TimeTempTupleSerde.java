package com.nestedtori.heatgen.serdes;

import com.nestedtori.heatgen.datatypes.*;
import java.util.Map;

import org.apache.kafka.common.serialization.Serde;

public class TimeTempTupleSerde implements Serde<TimeTempTuple> {
	private TimeTempTupleSerializer gls;
	private TimeTempTupleDeserializer glds;
	
	public void configure(Map<String, ?> configs, boolean isKey) {
		gls = new TimeTempTupleSerializer();
		glds = new TimeTempTupleDeserializer();
	}
	
	public TimeTempTupleSerializer serializer() { return gls; }
	public TimeTempTupleDeserializer deserializer() { return glds; }
	
	public void close() {}
}
