package stubs;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/*
 * The LogMonthMapper will map a log file output line to an IP/month pair
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 * 
 * Example output key: 96.7.4.14
 * Example output value: Apr
 */

public class LogMonthMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  //first, convert input value into String
	  String line = value.toString();
	  
	  //skip lines that are blank
	  if (line.trim().isEmpty()) return;
	  
	  //split parts by spaces
	  String[] parts = line.split("\\s+");
	  
	  //the first four parts of the line have the IP and month
	  //so if there are less than 4 parts, that is not enough info
	  if (parts.length < 4) return;
	  
	  //the ip is always the first index, and date is third index
	  //example: 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] turns into
	  //[96.7.4.14,-,-,[24/Apr/2011:04:20:11 -0400]]
	  String ip = parts[0];
	  String date = parts[3];
	  
	  //extract month from date by splitting by '/'
	  String[] dateElement = date.split("/");
	  
	  //there should be at least 2 elements because the date is the second element
	  if (dateElement.length < 2) return;
	  String month = dateElement[1];
	  
	  //emit output (ip, month)
	  context.write(new Text(ip), new Text(month));
	  
  }
}
