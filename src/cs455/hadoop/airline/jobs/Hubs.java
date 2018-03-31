package cs455.hadoop.airline.jobs;

import cs455.hadoop.airline.combiners.HubsCombiner;
import cs455.hadoop.airline.mappers.HubsMapper;
import cs455.hadoop.airline.mappers.TopMapper;
import cs455.hadoop.airline.reducers.TopReducer;
import cs455.hadoop.airline.utils.HubsCompositeKey;
import cs455.hadoop.airline.utils.TopWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Hubs {
  private final String tmpPath = "/tmp/hubs";

  Hubs(String input, String output)
      throws IOException, InterruptedException, ClassNotFoundException {
    // JOB 1 ----------------------------------------------------------
    // Maps hub to frequency of occurrence
    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "Hubs Intermediate");
    job1.setJarByClass(Hubs.class);

    job1.setMapperClass(HubsMapper.class);
    job1.setCombinerClass(HubsCombiner.class);
    //job1.setReducerClass(TopReducer.class);

    job1.setMapOutputKeyClass(HubsCompositeKey.class);
    job1.setMapOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job1, new Path(input));
    FileOutputFormat.setOutputPath(job1, new Path(tmpPath));

    job1.waitForCompletion(true);
    // JOB 2 -----------------------------------------------------------
    // Pulls the top X hubs from JOB 1

    Job job2 = Job.getInstance(conf, "Hubs");
    job2.setJarByClass(Hubs.class);

    job2.setMapperClass(TopMapper.class);
    job2.setReducerClass(TopReducer.class);

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(TopWritable.class);
    job2.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job2, new Path(tmpPath));
    FileOutputFormat.setOutputPath(job2, new Path(output));

    job2.waitForCompletion(true);
    job2.getCluster().getFileSystem().delete(new Path(tmpPath), true);
  }

  public static void main(String[] args) {
    try {
      new Hubs(args[0], args[1]);

    } catch (IOException ioe) {
      ioe.printStackTrace();
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    } catch (ClassNotFoundException cnfe) {
      cnfe.printStackTrace();
    }
  }
}
