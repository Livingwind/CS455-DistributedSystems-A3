package cs455.hadoop.airline.mappers;

import cs455.hadoop.airline.utils.DelayCompositeKey;
import cs455.hadoop.airline.utils.MeanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DelayMapper extends Mapper<LongWritable, Text, DelayCompositeKey, MeanWritable> {
  private int formatHH(String line) {
    String formatted = line;
    while(formatted.length() < 4) {
      formatted = "0" + formatted;
    }
    return Integer.parseInt(formatted.substring(0,2));
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] entries = value.toString().split(",");
    if(entries[0].equals("Year") || entries[14].equals("NA") || entries[15].equals("NA"))
      return;

    int hour = formatHH(entries[5]);
    if(hour == 24)
      hour = 0;
    long delays = Long.parseLong(entries[14]) + Long.parseLong(entries[15]);
    MeanWritable inter = new MeanWritable(1, delays);

    context.write(new DelayCompositeKey(DelayCompositeKey.TIME, hour), inter);

    int day = Integer.parseInt(entries[3]);
    context.write(new DelayCompositeKey(DelayCompositeKey.DAY, day), inter);

    int month = Integer.parseInt(entries[1]);
    context.write(new DelayCompositeKey(DelayCompositeKey.MONTH, month), inter);
  }
}
