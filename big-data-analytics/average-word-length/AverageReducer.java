package stubs;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  private DoubleWritable average = new DoubleWritable();

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

    int sum = 0;
    int count = 0;

    // Sum all word lengths and count how many words
    for (IntWritable value : values) {
      sum += value.get();
      count++;
    }

    // Compute average
    double avg = (double) sum / count;

    average.set(avg);

    // Emit (letter, average)
    context.write(key, average);
  }
}
