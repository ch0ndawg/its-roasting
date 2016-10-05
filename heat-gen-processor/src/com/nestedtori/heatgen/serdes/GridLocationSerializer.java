package com.nestedtori.heatgen.serdes;

import com.nestedtori.heatgen.datatypes.GridLocation;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

public class GridLocationSerializer implements Serializer<GridLocation> {

	private LongSerializer longSerializer;
	
	@Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        longSerializer = new LongSerializer();
    }
	
	@Override
	public byte[] serialize(String topic, GridLocation data) {
        if (data == null)
            return null;

        //byte[] iBytes = intSerializer.serialize(topic, data.i);
        //byte[] jBytes = intSerializer.serialize(topic, data.j);
        long hi = (long) data.i << 32;
        return longSerializer.serialize(topic, hi + data.j);
    }

    @Override
    public void close() {
        // nothing to do
    }
}

