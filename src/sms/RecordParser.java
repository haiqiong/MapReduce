package sms;

import org.apache.hadoop.io.Text;


public class RecordParser {
	private static final int MISSING = 9999;
	private String year;
	private int temp;
	private String quality;
	
	public void parse(Text record) {
		parse(record.toString());
	}
	
	public void parse(String record) {
		year = record.substring(15, 19);
		String airTemp;
		if (record.charAt(87) == '+') {
			airTemp = record.substring(88, 92);
		} else {
			airTemp = record.substring(87, 92);
		}
		temp = Integer.parseInt(airTemp);
		quality = record.substring(92, 93);
	}
	public boolean isValidTemp() {
		return temp != MISSING && quality.matches("[01459]") && temp <= 1000;
	}
	
	public boolean isMalformedTemp() {
		return temp != MISSING && quality.matches("[01459]") && temp > 1000;
	}
	public String getYear() {
		return year;
	}
	public int getTemp() {
		return temp;
	}
}
