package com.nestedtori.heatgen;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import com.nestedtori.heatgen.datatypes.TimeTempTuple;

public class HeatGenTimestampExtractor implements TimestampExtractor {
	public static long BEETHOVEN_TURNS_209 = 314159265359L; // replace with app start at main or ms since 1980;
	// actual dates are irrelevant so long as they are in the right event-time (embedded stamp) order
	@Override
	public long extract(ConsumerRecord<Object, Object> arg0) {
		// TODO Auto-generated method stub
		long timestampValue = ((TimeTempTuple) arg0.value()).time;
		return BEETHOVEN_TURNS_209 + timestampValue;
	}

}
