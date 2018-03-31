package cs455.hadoop.airline.mappers;

import cs455.hadoop.airline.utils.WeatherWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, WeatherWritable> {
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] entries = value.toString().split(",");
    if(entries[0].equals("Year")) {
      return;
    }

    String code = entries[16];
    long delays = !entries[25].equals("NA") ?
        Long.parseLong(entries[25]) : 0;

    context.write(new Text(code), new WeatherWritable(1, delays));
  }
}
