package stubs;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

/*
 * The CountReducer will reduce a string containing one IP address along with
 * an iterable collection of Text and emit the IP address and the total number
 * of hits from that IP address in that month
 * 
 * Example input:
 * "96.7.4.14, [Jan, Jan, Jan]"
 * 
 * Example output key: 96.7.4.14
 * Example output value: 3
 */

public class CountReducer extends Reducer<Text, Text, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  int hits = 0;
	  
	  for (Text month : values) {
		  hits++;
	  }
	  
	  context.write(key,  new IntWritable(hits));
  }
}
