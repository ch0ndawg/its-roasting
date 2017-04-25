package com.nestedtori.heatgen;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.BatchStatement;

import com.nestedtori.heatgen.datatypes.*;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.streams.processor.ProcessorContext;

import com.datastax.driver.core.*;

public class TempConsumer implements Runnable {
	public static int numCols;
	public static int numRows;
	public String[] args;
	// singleton ; number of columns is solely for the purpose of partitioning
	// N is not necessarily consistent with the actual number as given in the data itself
	//HeatGenProducer(int N) { numCols = N; }
	public TempConsumer(String[] args) {
		this.args = args;
		cassCluster = Cluster.builder()
				.addContactPoint(server)
				.addContactPoint(server2)
				.addContactPoint(server3)
				.addContactPoint(server4)
				.build();
		session = cassCluster.connect();
	}
	
	private Cluster cassCluster = null;
	private Session session = null;
	private String server = "ip-172-31-44-232";
	private String server2 = "ip-172-31-34-194";
	private String server3 = "ip-172-31-33-124";
	private String server4 = "ip-172-31-46-132";

	
	public void init(ProcessorContext context) {
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub

		Properties props = new Properties();
		
		//props.put(ConsumerConfig.PARTITIONER_CLASS_CONFIG, HeatGenPartitioner.class.getName());
      //  Serde<GridLocation> S = Serdes.serdeFrom(new GridLocationSerializer(), new GridLocationDeserializer());
      //  Serde<TimeTempTuple> GS = Serdes.serdeFrom(new TimeTempTupleSerializer(), new TimeTempTupleDeserializer());
      
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("group.id", "temp-consumer-client");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "200");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "com.nestedtori.heatgen.serdes.GridLocationDeserializer");
        props.put("value.deserializer", "com.nestedtori.heatgen.serdes.TimeTempTupleDeserializer");
        
		numCols = Integer.parseInt(args[1]);
		numRows = Integer.parseInt(args[2]);

		double leftX = (args.length < 6)? -10.0 : Double.parseDouble(args[5]);
		double rightX = (args.length < 7)? 10.0 : Double.parseDouble(args[6]);
		double bottomY =(args.length < 8)? -10.0 : Double.parseDouble(args[7]);
		double topY = (args.length < 9)? 10.0 : Double.parseDouble(args[8]);
		 
		// double C = (args.length < 10) ? 0.1875: Double.parseDouble(args[9]);
		
		double dx  = (rightX - leftX)/(numCols-1);
		double dy = (topY - bottomY)/(numCols-1);
		
		int timeUnit = 100;
        
		KafkaConsumer<GridLocation, TimeTempTuple> consumer = 
				new KafkaConsumer<GridLocation, TimeTempTuple>(props);
		
		PreparedStatement ps = session.prepare("insert into heatgen.temps (time,x_coord,y_coord,temp) values (?,?,?,?)");
		ps.setConsistencyLevel(ConsistencyLevel.ANY);
		try {
			consumer.subscribe(Arrays.asList("temp-output"));
			BatchStatement batch = new BatchStatement();
			
			while (true) {
		        ConsumerRecords<GridLocation, TimeTempTuple> records = consumer.poll(200);
		        System.out.println("Obtained " + records.count() + " records.");
		        int batchCount = 0;
		        for (ConsumerRecord<GridLocation, TimeTempTuple> record : records) {
		        	GridLocation k = record.key();
		        	TimeTempTuple value = record.value();
		        	double x = leftX + k.i * dx;
		     		double y = bottomY + k.j * dy;
		     		BoundStatement bs = ps.bind(value.time/timeUnit, x, y, value.val);
		     		batch.add(bs);
		     		batchCount++;
		     		if (batchCount >= 300) {
		     			session.executeAsync(batch);
		     			batchCount = 0;
		     			batch.clear();
		     		}
		     		
		         }
		         session.executeAsync(batch);
		     }
		} catch (WakeupException e) {
			// do nothing
		} finally {
			consumer.close();
			if (cassCluster != null) cassCluster.close();
		}
	}

}
