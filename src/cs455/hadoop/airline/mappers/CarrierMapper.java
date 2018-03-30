package cs455.hadoop.airline.mappers;

import cs455.hadoop.airline.utils.MeanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CarrierMapper extends Mapper<LongWritable, Text, Text, MeanWritable> {
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] entries = value.toString().split(",");
    if(entries[0].equals("Year") || entries[14].equals("NA") || entries[15].equals("NA"))
      return;

    String carrier = entries[8];
    long delays = Long.parseLong(entries[14]) + Long.parseLong(entries[15]);
    if(delays == 0) {
      return;
    }

    context.write(new Text(carrier), new MeanWritable(1, delays));
  }
}
