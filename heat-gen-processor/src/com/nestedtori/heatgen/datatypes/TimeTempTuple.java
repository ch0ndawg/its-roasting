package com.nestedtori.heatgen.datatypes;

public class TimeTempTuple {
	public long time;
	public double val;
	public TimeTempTuple(long t, double v) {
		time = t;
		val = v;
	}
	
	@Override
	public String toString() {
		return "Time: " + time + " Temp: " + val;
	}
}
