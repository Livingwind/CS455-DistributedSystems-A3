package cs455.hadoop.airline.jobs;

import cs455.hadoop.airline.combiners.AgeCombiner;
import cs455.hadoop.airline.combiners.CSVMeanCombiner;
import cs455.hadoop.airline.mappers.AgeMapper;
import cs455.hadoop.airline.mappers.AgeMeanMapper;
import cs455.hadoop.airline.mappers.AgeSuppMapper;
import cs455.hadoop.airline.reducers.AgeInterReducer;
import cs455.hadoop.airline.reducers.AgeMeanReducer;
import cs455.hadoop.airline.utils.AgeWritable;
import cs455.hadoop.airline.utils.HadoopUtils;
import cs455.hadoop.airline.utils.MeanWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Age{
  private static final String tmpDir = "/tmp/age";
  private static final String suppDir = "/data/supplementary/plane-data.csv";
  private Configuration conf;
  private List<Job> jobChain = new ArrayList<>();

  public Age(String input, String output)
      throws IOException, InterruptedException, ClassNotFoundException {
    conf = new Configuration();
    Path inpath = new Path(input);
    Path suppath = new Path(suppDir);
    Path tmppath = HadoopUtils.clearPath(conf, tmpDir);
    Path outpath = HadoopUtils.clearPath(conf, output);

    // JOB 1 ----------------------------------------------------------
    // Combine input and supplementary data into tail: {manufacture year, flight year, delay}
    Job job1 = Job.getInstance(conf, "Age intermediate");
    job1.setJarByClass(Age.class);

    MultipleInputs.addInputPath(job1, suppath, TextInputFormat.class, AgeSuppMapper.class);
    MultipleInputs.addInputPath(job1, inpath, TextInputFormat.class, AgeMapper.class);
    job1.setCombinerClass(AgeCombiner.class);
    job1.setReducerClass(AgeInterReducer.class);
    FileOutputFormat.setOutputPath(job1, tmppath);

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(AgeWritable.class);
    job1.setOutputValueClass(Text.class);

    jobChain.add(job1);
    // JOB 2 ----------------------------------------------------------
    // Map plane age to mean delay
    Job job2 = Job.getInstance(conf, "Age");
    job2.setJarByClass(Age.class);

    FileInputFormat.setInputPaths(job2, tmppath);
    job2.setMapperClass(AgeMeanMapper.class);
    job2.setReducerClass(AgeMeanReducer.class);
    FileOutputFormat.setOutputPath(job2, outpath);

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(MeanWritable.class);
    job2.setOutputValueClass(Text.class);

    jobChain.add(job2);
  }

  public void run()
      throws IOException, InterruptedException, ClassNotFoundException {
    for(Job j: jobChain) {
      j.waitForCompletion(true);
    }
  }

  public static void main(String[] args) {
    try {
      Age age = new Age(args[0], args[1]);
      age.run();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
