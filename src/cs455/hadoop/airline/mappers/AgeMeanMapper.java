package cs455.hadoop.airline.mappers;

import cs455.hadoop.airline.utils.MeanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AgeMeanMapper extends Mapper<LongWritable, Text, Text, MeanWritable> {

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] entries = value.toString().split("\\s+");

    int age = Integer.parseInt(entries[1]);
    int count = Integer.parseInt(entries[2]);
    long sum = Long.parseLong(entries[3]);

    context.write(new Text(age > 20?"Old":"New"), new MeanWritable(count, sum));
  }
}
