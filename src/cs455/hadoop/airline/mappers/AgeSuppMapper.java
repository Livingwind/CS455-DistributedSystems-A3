package cs455.hadoop.airline.mappers;

import cs455.hadoop.airline.utils.AgeWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AgeSuppMapper extends Mapper<LongWritable, Text, Text, AgeWritable> {

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String [] entries = value.toString().split(",");
    if(entries.length < 9 || entries[0].equals("tailnum")) {
      return;
    }

    String tailnum = entries[0];
    int age;
    try {
      age = Integer.parseInt(entries[8]);
      if(age == 0) {
        return;
      }
    } catch (NumberFormatException nfe) {
      return;
    }

    context.write(new Text(tailnum), new AgeWritable(age));
  }
}
