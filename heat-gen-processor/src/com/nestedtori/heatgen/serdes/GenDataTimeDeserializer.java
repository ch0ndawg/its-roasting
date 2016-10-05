package com.nestedtori.heatgen.serdes;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;

import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.nestedtori.heatgen.datatypes.GenDataTime;

public class GenDataTimeDeserializer implements Deserializer<GenDataTime> {
	private LongDeserializer longDeserializer;
	private DoubleDeserializer doubleDeserializer;
	
	@Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        longDeserializer = new LongDeserializer();
        doubleDeserializer = new DoubleDeserializer();
    }
	
	@Override
	public GenDataTime deserialize(String topic, byte[] data) {
        if (data == null)
            return null;

        byte[] timeBytes = Arrays.copyOfRange(data, 0, 8);
        byte[] valBytes = Arrays.copyOfRange(data, 8, 16);
        Long t = longDeserializer.deserialize(topic, timeBytes);
        Double v = doubleDeserializer.deserialize(topic, valBytes);
        return new GenDataTime(t,v);
    }

    @Override
    public void close() {
        // nothing to do
    }
}