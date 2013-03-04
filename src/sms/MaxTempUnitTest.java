package sms;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;


public class MaxTempUnitTest {
//	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
//	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
//	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
//	
//	MaxTempMapper mapper = new MaxTempMapper();
//	MaxTempReducer reducer = new MaxTempReducer();
//	mapDriver = MapDriver.newMapDriver(mapper);
//	reduceDriver = ReduceDriver.newReduceDriver(reducer);
//	mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	@Test
	public void missValueTest() {
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" 
				+ "99999V0203201N00261220001CN9999999N9+99991+99999999999");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
	    .withMapper(new MaxTempMapper())
	    .withInputValue(value)
	    .withOutput(new Text("1950"), new IntWritable(-11))
	    .runTest();
	}
	
	@Test
	  public void returnsMaximumIntegerInValues() throws IOException,
	      InterruptedException {
	    new ReduceDriver<Text, IntWritable, Text, IntWritable>()
	      .withReducer(new MaxTempReducer())
	      .withInputKey(new Text("1950"))
	      .withInputValues(Arrays.asList(new IntWritable(10), new IntWritable(5)))
	      .withOutput(new Text("1950"), new IntWritable(10))
	      .runTest();
	  }
}
