package cs455.hadoop.airline.mappers;

import cs455.hadoop.airline.utils.HubsCompositeKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HubsMapper extends Mapper<LongWritable, Text, HubsCompositeKey, IntWritable> {
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] entries = value.toString().split(",");
    if(entries[0].equals("Year")) {
      return;
    }

    String year = entries[0];
    String hub = entries[16];

    context.write(new HubsCompositeKey(year, hub), new IntWritable(1));
  }
}
