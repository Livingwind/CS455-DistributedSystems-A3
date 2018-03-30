package cs455.hadoop.airline.mappers;

import cs455.hadoop.airline.utils.CarrierWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CarrierInterMapper extends Mapper<LongWritable, Text, Text, CarrierWritable> {

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] entries = value.toString().split("[,\t]+");
    String code = entries[0];
    int count = Integer.parseInt(entries[1]);
    long sum = Long.parseLong(entries[2]);

    context.write(new Text(code), new CarrierWritable(count, sum));
  }
}
