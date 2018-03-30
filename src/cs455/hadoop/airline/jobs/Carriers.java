package cs455.hadoop.airline.jobs;

import cs455.hadoop.airline.combiners.CSVMeanCombiner;
import cs455.hadoop.airline.mappers.CarrierInterMapper;
import cs455.hadoop.airline.mappers.CarrierMapper;
import cs455.hadoop.airline.mappers.CarrierSuppMapper;
import cs455.hadoop.airline.reducers.CarrierReducer;
import cs455.hadoop.airline.utils.CarrierWritable;
import cs455.hadoop.airline.utils.MeanWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Carriers {
  private static final String tmpDir = "/tmp/carriers";
  private Configuration conf;
  private Job job1;
  private Job job2;

  public Carriers(String input, String output)
      throws IOException, InterruptedException, ClassNotFoundException {
    conf = new Configuration();
    Path inpath = new Path(input);
    Path outpath = new Path(output);
    Path tmppath = new Path(tmpDir);
    Path supCarrierData = new Path("/data/supplementary/carriers.csv");

    FileSystem fs = FileSystem.get(conf);
    if(fs.exists(outpath)) {
      fs.delete(outpath, true);
    }
    if(fs.exists(tmppath)) {
      fs.delete(tmppath, true);
    }

    // JOB 1 ----------------------------------------------------------
    // Maps carrier codes to delay information
    job1 = Job.getInstance(conf, "Carriers Intermediate");
    job1.setJarByClass(Carriers.class);

    job1.setMapperClass(CarrierMapper.class);
    job1.setCombinerClass(CSVMeanCombiner.class);

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(MeanWritable.class);

    FileInputFormat.addInputPath(job1, inpath);
    FileOutputFormat.setOutputPath(job1, tmppath);

    // JOB 2 -----------------------------------------------------------
    // Combines the results of JOB 1 with the supplementary carrier data
    job2 = Job.getInstance(conf, "Carriers");
    job2.setJarByClass(Carriers.class);

    MultipleInputs.addInputPath(job2, supCarrierData, TextInputFormat.class, CarrierSuppMapper.class);
    MultipleInputs.addInputPath(job2, tmppath, TextInputFormat.class, CarrierInterMapper.class);
    job2.setReducerClass(CarrierReducer.class);

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(CarrierWritable.class);
    job2.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job2, outpath);
  }

  public void run() throws IOException, InterruptedException, ClassNotFoundException {
    job1.waitForCompletion(true);
    job2.waitForCompletion(true);
  }

  public static void main(String[] args) {
    try {
      Carriers carriers = new Carriers(args[0], args[1]);
      carriers.run();

    } catch (IOException ioe) {
      ioe.printStackTrace();
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    } catch (ClassNotFoundException cnfe) {
      cnfe.printStackTrace();
    }
  }
}
