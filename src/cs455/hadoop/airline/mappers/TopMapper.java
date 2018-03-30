package cs455.hadoop.airline.mappers;

import cs455.hadoop.airline.utils.TopWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopMapper extends Mapper<LongWritable, Text, Text, TopWritable> {
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] entries = value.toString().split("[:\\s+]");
    String year = entries[0];
    String hub = entries[1];
    int count = Integer.parseInt(entries[2]);

    context.write(new Text(year), new TopWritable(hub, count));
  }
}
