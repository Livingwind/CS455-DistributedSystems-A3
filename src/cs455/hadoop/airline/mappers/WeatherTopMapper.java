package cs455.hadoop.airline.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WeatherTopMapper extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    context.write(new Text("TOP"), value);
  }
}
