package stubs;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;

/* Instructions:
 * Modify your driver code to specify that you want 12 Reducers. 
 * Configure your job to use your custom Partitioner. 
 */

public class ProcessLogs {

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: ProcessLogs <input dir> <output dir>\n");
      System.exit(-1);
    }

    // Instantiate a Job object for your job's configuration. 
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Process Logs");

    job.setJarByClass(ProcessLogs.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(LogMonthMapper.class);
    job.setReducerClass(CountReducer.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    //todo: implement reducers and partitioner
    job.setPartitionerClass(MonthPartitioner.class);
    int numberOfReducers = 12;
    job.setNumReduceTasks(numberOfReducers);

    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}
