package cs455.hadoop.airline.mappers;

import cs455.hadoop.airline.utils.AgeWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AgeMapper extends Mapper<LongWritable, Text, Text, AgeWritable> {
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] entries = value.toString().split(",");
    if(entries[0].equals("Year") || entries[10].equals("NA") ||
        entries[14].equals("NA") || entries[15].equals("NA"))
      return;

    int year = Integer.parseInt(entries[0]);
    String tailnum = entries[10];
    long delays = Long.parseLong(entries[14]) + Long.parseLong(entries[15]);
    if(delays == 0) {
      return;
    }

    context.write(new Text(tailnum), new AgeWritable(year, 1, delays));
  }
}
