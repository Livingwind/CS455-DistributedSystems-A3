package cs455.hadoop.airline.combiners;

import cs455.hadoop.airline.utils.AgeWritable;
import cs455.hadoop.airline.utils.WeatherWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherCombiner extends Reducer<Text, WeatherWritable, Text, WeatherWritable> {
  @Override
  protected void reduce(Text key, Iterable<WeatherWritable> values, Context context) throws IOException, InterruptedException {
    String city = null;
    int count = 0;
    long sum = 0;

    for(WeatherWritable value: values) {
      if(value.city == null) {
        count += value.count;
        sum += value.sum;
      } else {
        city = value.city;
      }
    }

    context.write(key, new WeatherWritable(city, count, sum));
  }
}
