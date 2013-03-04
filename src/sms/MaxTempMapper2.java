package sms;

/** filter malformatted input with count.
 * @author haiqiongyao
 * 2/28/2013
 */

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MaxTempMapper2 extends Mapper<LongWritable, Text, Text, IntWritable>{
private RecordParser parser = null;
	
	enum HighTemp {
		MALFORMED;
	}
	
	public MaxTempMapper2() {
		parser = new RecordParser();
	}
	@Override
	public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException {
		parser.parse(value);
		if (parser.isValidTemp()) {
			context.write(new Text(parser.getYear()), new IntWritable(parser.getTemp()));
		} else if (parser.isMalformedTemp()) {
			System.err.println("ignore corrupt input: " + parser.getTemp());
			context.getCounter(HighTemp.MALFORMED).increment(1);
		}
	}
}
