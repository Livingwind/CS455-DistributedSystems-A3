package cs455.hadoop.airline.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;

public abstract class AirlineJob {
  protected Configuration conf = new Configuration();
  protected ArrayList<Job> jobChain = new ArrayList<>();

  public void run()
    throws IOException, InterruptedException, ClassNotFoundException {
    for(Job j: jobChain) {
      j.waitForCompletion(true);
    }
  }
}
