package sms;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private static final int MISSING = 9999;
	public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException {
		String line = value.toString();
		String year = line.substring(15, 19);
		int temp;
		if (line.charAt(87) == '+') {
			temp = Integer.parseInt(line.substring(88, 92));
		} else {
			temp = Integer.parseInt(line.substring(87, 92));
		}
		String quality = line.substring(92, 93);
		if (temp != MISSING && quality.matches("[01459]")) {
			context.write(new Text(year), new IntWritable(temp));
		}
	}
}
