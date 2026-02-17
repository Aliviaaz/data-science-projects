package stubs;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  private Text firstLetter = new Text();
  private IntWritable wordLength = new IntWritable();

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    // Convert line to string
    String line = value.toString();

    // Split on one or more non-word chars
    String[] words = line.split("\\W+");

    for (String word : words) {
      // Ignore empty strings
      if (word.length() > 0) {

        // Extract first character (case-sensitive)
        String letter = word.substring(0, 1);

        // Set output key and value
        firstLetter.set(letter);
        wordLength.set(word.length());

        // Emit (firstLetter, wordLength)
        context.write(firstLetter, wordLength);
      }
    }
  }
}
