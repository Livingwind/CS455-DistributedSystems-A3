package cs455.hadoop.airline.reducers;

import cs455.hadoop.airline.utils.WeatherWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherReducer extends Reducer<Text, WeatherWritable, Text, Text> {
  @Override
  protected void reduce(Text key, Iterable<WeatherWritable> values, Context context)
      throws IOException, InterruptedException {
    String city = null;
    int count = 0;
    long sum = 0;

    for(WeatherWritable value: values) {
      if(value.city != null) {
        city = value.city;
      }
      count += value.count;
      sum += value.sum;
    }

    if(count == 0 || city.equals("NA")) {
      return;
    }

    String out = String.format(
      ",%.2f", (float)sum/count
    );

    context.write(new Text(city), new Text(out));
  }
}
