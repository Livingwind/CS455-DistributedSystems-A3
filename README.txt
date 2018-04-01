Each file in the cs455.hadoop.airline.jobs package can be run as a main for limited runs and all
  follow the convention: cs455.hadoop.airline.<Class> input output
An additional main is present in AllJobs which will run each job back to back following the standard
  convention: cs455.hadoop.airline.AllJobs input output
  **The program will append /<job> to the output path specified for easy analysis.
  **It's important to format your output path like ""/path/to/files" to prevent "/path/to/files//<job>"

Q7: My analysis program looks at security delays by month over the year 2003-2008 (Security delay started
      being logged after June 2003) with an attempt to link times of the year to violent security threats.
      The bulk of the analysis happens in AirportSecurityAnalysis.ods with data provided by the Security.java
      Hadoop job.


/analysis/ contains the answers to Q1-Q6 as well as the deeper analysis into Q7

cs455.hadoop.airline.combiners:
  Contains combiners matching the naming of their associated Hadoop job

cs455.hadoop.airline.enums:
  Constants referring to Day and Month. Provides a nice abstraction for output.

cs455.hadoop.airline.jobs:
  AirlineJob.java - Base class which each job extends. Has a run() method which runs each job in the chain.
  AllJobs.java - Coalesces each job into one main
  Age.java - Answers Q5
  Carriers.java - Answers Q4
  Delay.java - Answers Q1/Q2
  Hubs.java - Answers Q3
  Security.java - Answers Q7 (further analytics provided by analysis/AirportSecurityAnalysis.ods)
  Weather.java - Answer Q6

cs455.hadoop.airline.mappers:
  Contains mappers matching their associated jobs.
  Some are used to process supplementary data and are named <JOB>SuppMapper.java

cs455.hadoop.airline.reducers:
  Contains reducers matching their associated jobs.
  Some reducers have different functionality, WeatherTopReducer.java which takes the top 10 results
    is an example

cs455.hadoop.airline.utils:
  HadoopUtils.java - Has helper methods for common actions within the Hadoop jobs
  The rest of the java files are either custom Writable's or CompositeKey's matching
    the Hadoop jobs.