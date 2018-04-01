package cs455.hadoop.airline.jobs;

import cs455.hadoop.airline.combiners.SecurityCombiner;
import cs455.hadoop.airline.mappers.SecurityMapper;
import cs455.hadoop.airline.reducers.SecurityReducer;
import cs455.hadoop.airline.utils.HadoopUtils;
import cs455.hadoop.airline.utils.MeanWritable;
import cs455.hadoop.airline.utils.SecurityCompositeKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class Security extends AirlineJob {
  public Security(String input, String output)
      throws IOException, InterruptedException, ClassNotFoundException {
    Path inpath = new Path(input);
    Path outpath = HadoopUtils.clearPath(conf, output);

    // JOB 1 ----------------------------------------------------------
    // Calculate the mean security delay in partitions of year:month
    Job job1 = Job.getInstance(conf, "Security");
    job1.setJarByClass(Security.class);

    FileInputFormat.setInputPaths(job1, inpath);
    job1.setMapperClass(SecurityMapper.class);
    job1.setCombinerClass(SecurityCombiner.class);
    job1.setReducerClass(SecurityReducer.class);
    FileOutputFormat.setOutputPath(job1, outpath);

    job1.setMapOutputKeyClass(SecurityCompositeKey.class);
    job1.setMapOutputValueClass(MeanWritable.class);
    job1.setOutputValueClass(Text.class);

    jobChain.add(job1);
  }

  public static void main(String[] args) {
    try {
      Security s = new Security(args[0], args[1]);
      s.run();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
