package cs455.hadoop.airline.jobs;

import java.util.ArrayList;

public class AllJobs {
  public static void main(String[] args) {
    try {
      ArrayList<AirlineJob> allJobs = new ArrayList<>();
      allJobs.add(new Age(args[0], args[1] + "/age"));
      allJobs.add(new Carriers(args[0], args[1] + "/carriers"));
      allJobs.add(new Delay(args[0], args[1] + "/delay"));
      allJobs.add(new Hubs(args[0], args[1] + "/hubs"));
      allJobs.add(new Security(args[0], args[1] + "/security"));
      allJobs.add(new Weather(args[0], args[1] + "/weather"));

      for(AirlineJob job: allJobs) {
        job.run();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
