package sms;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;


public class SmsUnitTest {
//	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
//	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
//	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
//	
//	SmsMapper mapper = new SmsMapper();
//	mapDriver = MapDriver.newMapDriver(mapper);
//	
	@Test
	public void smsMapperTest() {
		Text value = new Text("655209;1;796764372490213;804422938115889;6");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
	      .withMapper(new SmsMapper())
	      .withInputValue(value)
	      .runTest();
	}
}
