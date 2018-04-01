package cs455.hadoop.airline.mappers;

import cs455.hadoop.airline.enums.Month;
import cs455.hadoop.airline.utils.MeanWritable;
import cs455.hadoop.airline.utils.SecurityCompositeKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecurityMapper extends Mapper<LongWritable, Text, SecurityCompositeKey, MeanWritable> {
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] entries = value.toString().split(",");
    if(entries[0].equals("Year")) {
      return;
    }

    int year = Integer.parseInt(entries[0]);
    Month month = Month.value(Integer.parseInt(entries[1]));

    long delay = !entries[27].equals("NA") ?
        Long.parseLong(entries[27]) : 0;

    context.write(new SecurityCompositeKey(year, month), new MeanWritable(1, delay));
  }
}
