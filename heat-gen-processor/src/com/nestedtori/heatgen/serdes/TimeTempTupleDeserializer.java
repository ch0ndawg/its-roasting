package com.nestedtori.heatgen.serdes;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;

import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.nestedtori.heatgen.datatypes.TimeTempTuple;

public class TimeTempTupleDeserializer implements Deserializer<TimeTempTuple> {
	private LongDeserializer longDeserializer;
	private DoubleDeserializer doubleDeserializer;
	
	public TimeTempTupleDeserializer() {
        longDeserializer = new LongDeserializer();
        doubleDeserializer = new DoubleDeserializer();
	}
	@Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }
	
	@Override
	public TimeTempTuple deserialize(String topic, byte[] data) {
        if (data == null)
            return null;

        byte[] timeBytes = Arrays.copyOfRange(data, 0, 8);
        byte[] valBytes = Arrays.copyOfRange(data, 8, 16);
        Long t = longDeserializer.deserialize(topic, timeBytes);
        Double v = doubleDeserializer.deserialize(topic, valBytes);
        return new TimeTempTuple(t,v);
    }

    @Override
    public void close() {
        // nothing to do
    }
}