package com.nestedtori.heatgen.serdes;

import java.util.Map;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.Serializer;

import com.nestedtori.heatgen.datatypes.TimeTempTuple;

public class TimeTempTupleSerializer implements Serializer<TimeTempTuple> {
	private LongSerializer longSerializer;
	private DoubleSerializer doubleSerializer;
	
	@Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        longSerializer = new LongSerializer();
        doubleSerializer = new DoubleSerializer();
    }
	
	@Override
	public byte[] serialize(String topic, TimeTempTuple data) {
        if (data == null)
            return null;

        byte[] timeBytes = longSerializer.serialize(topic, data.time);
        byte[] valBytes = doubleSerializer.serialize(topic, data.val);
        byte[] result = new byte[timeBytes.length + valBytes.length];
        int i=0;
        for (; i< timeBytes.length; i++) {
        	result[i] = timeBytes[i];
        }
        for (int j=0; j<valBytes.length; j++) {
        	result[i+j] = valBytes[j];
        }
        return result;
    }

    @Override
    public void close() {
        // nothing to do
    }
}
