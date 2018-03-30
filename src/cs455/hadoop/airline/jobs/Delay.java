package cs455.hadoop.airline.jobs;

import cs455.hadoop.airline.combiners.MeanCombiner;
import cs455.hadoop.airline.mappers.DelayMapper;
import cs455.hadoop.airline.reducers.MeanReducer;
import cs455.hadoop.airline.utils.DelayCompositeKey;
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

public class Delay {
  private final Job job;

  Delay(String input, String output) throws IOException {
    Configuration conf = new Configuration();
    job = Job.getInstance(conf, "Delay");
    job.setJarByClass(Delay.class);

    job.setMapperClass(DelayMapper.class);
    job.setCombinerClass(MeanCombiner.class);
    job.setReducerClass(MeanReducer.class);

    job.setMapOutputKeyClass(DelayCompositeKey.class);
    job.setMapOutputValueClass(MeanWritable.class);
    job.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
  }

  public TreeMap<String, Double> collectResults() throws IOException, InterruptedException {
    final FileSystem fs = job.getCluster().getFileSystem();
    Path outpath = new Path(FileOutputFormat.getOutputPath(job).toString() + "/part-r-00000");
    FSDataInputStream in = fs.open(outpath);
    byte[] bs = new byte[in.available()];
    in.read(bs);

    String[] data = new String(bs).split("\n");
    TreeMap<String, Double> map = new TreeMap<>();
    for(String line: data) {
      String[] splits = line.split("\\s+");
      map.put(splits[0], Double.parseDouble(splits[1]));
    }

    return map;
  }

  public String best(TreeMap<String, Double> map) {
    String best = map.firstKey();
    for(String key: map.keySet()) {
      if(map.get(key) < map.get(best)) {
        best = key;
      }
    }

    return best;
  }
  public String worst(TreeMap<String, Double> map) {
    String best = map.firstKey();
    for(String key: map.keySet()) {
      if(map.get(key) > map.get(best)) {
        best = key;
      }
    }

    return best;
  }

  public static void main(String[] args) {
    try {
      Delay tod = new Delay(args[0], args[1]);
      tod.job.waitForCompletion(true);

      //TreeMap<String, Double> map = tod.collectResults();

      //System.out.println("Best time of day to fly: " + tod.best(map) + ":00");
      //System.out.println("Worst time of day to fly: " + tod.worst(map) + ":00");

    } catch (IOException ioe) {
      ioe.printStackTrace();
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    } catch (ClassNotFoundException cnfe) {
      cnfe.printStackTrace();
    }
  }
}
