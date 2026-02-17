package stubs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 

public class AvgWordLength {

  public static void main(String[] args) throws Exception {

    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length != 2) {
      System.out.printf("Usage: AvgWordLength <input dir> <output dir>\n");
      System.exit(-1);
    }

    /*
     * Instantiate a Job object for your job's configuration. 
     */
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "avgwordlength");

    
    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running 
     * mapper and reducer tasks.
     */
    job.setJarByClass(AvgWordLength.class);
    
    /*
     * Set Mapper and Reducer classes
     */
    job.setMapperClass(LetterMapper.class);
    job.setReducerClass(AverageReducer.class);

    /*
     * Set intermediate (map output) key/value types
     */
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    /*
     * Set final output key/value types
     */
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    
    /*
     * Set input and output paths
     */
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}

