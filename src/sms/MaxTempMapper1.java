package sms;

/**detect malformatted input with count;
 * @author haiqiongyao
 * 2/28/2013
 */

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempMapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{
	private RecordParser parser = null;
	
	enum HighTemp {
		OVER_100;
	}
	
	public MaxTempMapper1() {
		parser = new RecordParser();
	}
	@Override
	public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException {
		parser.parse(value);
		if (parser.isValidTemp()) {
			if (parser.getTemp() > 1000) {
				System.err.printf("temp is higher than 100 as ", parser.getTemp());
				context.setStatus("temp is high, refer to log.");
				context.getCounter(HighTemp.OVER_100).increment(1);
			}
			context.write(new Text(parser.getYear()), new IntWritable(parser.getTemp()));
		}
	}
}
