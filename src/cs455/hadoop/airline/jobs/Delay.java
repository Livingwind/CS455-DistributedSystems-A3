package cs455.hadoop.airline.jobs;

import cs455.hadoop.airline.combiners.MeanCombiner;
import cs455.hadoop.airline.mappers.DelayMapper;
import cs455.hadoop.airline.reducers.MeanReducer;
import cs455.hadoop.airline.utils.DelayCompositeKey;
import cs455.hadoop.airline.utils.HadoopUtils;
import cs455.hadoop.airline.utils.MeanWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.TreeMap;

public class Delay extends AirlineJob {
  Delay(String input, String output) throws IOException {
    Job job = Job.getInstance(conf, "Delay");
    job.setJarByClass(Delay.class);

    job.setMapperClass(DelayMapper.class);
    job.setCombinerClass(MeanCombiner.class);
    job.setReducerClass(MeanReducer.class);

    job.setMapOutputKeyClass(DelayCompositeKey.class);
    job.setMapOutputValueClass(MeanWritable.class);
    job.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, HadoopUtils.clearPath(conf, output));

    jobChain.add(job);
  }

  public static void main(String[] args) {
    try {
      Delay tod = new Delay(args[0], args[1]);
      tod.run();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    } catch (ClassNotFoundException cnfe) {
      cnfe.printStackTrace();
    }
  }
}
