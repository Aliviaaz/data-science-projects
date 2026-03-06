package stubs;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; 
import org.apache.hadoop.fs.Path;

/*
 * Input file format:
 * Each line contains line number, separator (tab), value: the line of the text
 * Instructions:
 * This format can be read directly using the KeyValueTextInputFormat class provided
 * in the Hadoop API. This input format presents each line as one record to your Mapper,
 * with the part before the tab character as the key, and the part after the tab as the value.
 * For each word, the index should have a list of all the locations where the word appears.
 */

public class IndexMapper extends Mapper<Text, Text, Text, Text> {

  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {
	  //retrieve file name, which is the name of the play, using Context
	  FileSplit fileSplit = (FileSplit) context.getInputSplit();
	  Path path = fileSplit.getPath();
	  String filename = path.getName();
	  //store line number
	  String lineNumber = key.toString();
	  //store words in line
	  String[] words = value.toString().split("\\W+");
	  
	  //for every word, emit (word, location)
	  //location = filename@linenumber 
	  //example: honeysuckle, midsummernightsdream@2175
	  for (String word : words) {
	    if (word.length() > 0) {
	    	context.write(new Text(word), new Text(filename + "@" + lineNumber));
	    }
	  }
  }
}