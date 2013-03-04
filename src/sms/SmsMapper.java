package sms;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SmsMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
//	private Text status = new Text();
	
	private final static IntWritable ADD_ONE = new IntWritable(1);
	
	protected void map(LongWritable key, Text value, Context context) 
	throws IOException, InterruptedException{
		String[] line = value.toString().split(";");
		String status;
		if (Integer.parseInt(line[1]) == 1) {
//			status.setData(line[4]);
			status = line[4];
			context.write(new Text(status), ADD_ONE);
		}
	}
}
