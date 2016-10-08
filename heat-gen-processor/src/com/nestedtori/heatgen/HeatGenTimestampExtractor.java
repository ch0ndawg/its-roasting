package com.nestedtori.heatgen;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import com.nestedtori.heatgen.datatypes.TimeTempTuple;

public class HeatGenTimestampExtractor implements TimestampExtractor {
	@Override
	public long extract(ConsumerRecord<Object, Object> arg0) {
		// TODO Auto-generated method stub
		long timestampValue = ((TimeTempTuple) arg0.value()).time;
		return timestampValue;
	}

}
