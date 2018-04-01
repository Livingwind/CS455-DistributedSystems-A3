package cs455.hadoop.airline.jobs;

import cs455.hadoop.airline.combiners.WeatherCombiner;
import cs455.hadoop.airline.mappers.WeatherMapper;
import cs455.hadoop.airline.mappers.WeatherSuppMapper;
import cs455.hadoop.airline.mappers.WeatherTopMapper;
import cs455.hadoop.airline.reducers.WeatherReducer;
import cs455.hadoop.airline.reducers.WeatherTopReducer;
import cs455.hadoop.airline.utils.HadoopUtils;
import cs455.hadoop.airline.utils.WeatherWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Weather extends AirlineJob {
  private static final String tmpDir = "/tmp/weather";
  private static final String suppDir = "/data/supplementary/airports.csv";

  public Weather(String input, String output)
      throws IOException, InterruptedException, ClassNotFoundException {
    Path inpath = new Path(input);
    Path suppath = new Path(suppDir);
    Path tmppath = HadoopUtils.clearPath(conf, tmpDir);
    Path outpath = HadoopUtils.clearPath(conf, output);

    // JOB 1 ----------------------------------------------------------
    // Combine input and supplementary data into city: mean
    Job job1 = Job.getInstance(conf, "Weather intermediate");
    job1.setJarByClass(Weather.class);

    MultipleInputs.addInputPath(job1, suppath, TextInputFormat.class, WeatherSuppMapper.class);
    MultipleInputs.addInputPath(job1, inpath, TextInputFormat.class, WeatherMapper.class);
    job1.setCombinerClass(WeatherCombiner.class);
    job1.setReducerClass(WeatherReducer.class);
    FileOutputFormat.setOutputPath(job1, tmppath);

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(WeatherWritable.class);
    job1.setOutputValueClass(Text.class);

    jobChain.add(job1);
    // JOB 2 ----------------------------------------------------------
    // Pull out the top 10 from JOB 1
    Job job2 = Job.getInstance(conf, "Weather top 10");
    job2.setJarByClass(Weather.class);

    FileInputFormat.setInputPaths(job2, tmppath);
    job2.setMapperClass(WeatherTopMapper.class);
    job2.setReducerClass(WeatherTopReducer.class);
    FileOutputFormat.setOutputPath(job2, outpath);

    job2.setMapOutputKeyClass(Text.class);

    jobChain.add(job2);
  }

  public static void main(String[] args) {
    try {
      Weather w = new Weather(args[0], args[1]);
      w.run();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
