package com.nestedtori.heatgen;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import com.nestedtori.heatgen.datatypes.GridLocation;
import com.nestedtori.heatgen.datatypes.TimeTempTuple;

public class SaveToCassandraTransformer implements Transformer<GridLocation,TimeTempTuple, KeyValue<GridLocation,TimeTempTuple>> {
	
	private Cluster cassCluster = null;
	private Session session = null;
	private String server = null;
	private double leftX = -10.0;
	private double rightX = 10.0;
	private double bottomY = -10.0;
	private double topY = -10.0;
	double dx;
	double dy;
	
	public SaveToCassandraTransformer(String server, double leftX, double rightX, double bottomY, double topY) {
		this.server = server;
		this.leftX = leftX;
		this.rightX = rightX;
		this.bottomY = bottomY;
		this.topY = topY;
		dx = (rightX - leftX)/(HeatGenStreamProcessor.numCols - 1);
		dy = (topY - bottomY)/(HeatGenStreamProcessor.numRows - 1);
	}
	
	public void init(ProcessorContext context) {
		cassCluster = Cluster.builder().addContactPoint(server).build();
		session = cassCluster.connect();
	}
	
	@Override
	public KeyValue<GridLocation,TimeTempTuple> transform(GridLocation key, TimeTempTuple value) {
		double x = leftX + key.i * dx;
		double y = bottomY + key.j * dy;
		String jText = "{\"time\": " + value.time + ", \"x_coord\": " + x 
				        + ", \"y_coord\": " + y + ", \"temp\": " + value.val + "}" ; 
		session.execute("insert into heatgen.temps JSON '" + jText + "'"); 
		return new KeyValue<>(key, value); // return k,v as is
	}
	
	public void close() {
		if (cassCluster != null) cassCluster.close();
	}
	
	public KeyValue<GridLocation,TimeTempTuple> punctuate(long timestamp) {
		return new KeyValue<>(new GridLocation(0,0), new TimeTempTuple(0,0.0));
	}
}
