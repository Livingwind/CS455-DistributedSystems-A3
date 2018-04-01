package cs455.hadoop.airline.reducers;

import cs455.hadoop.airline.utils.MeanWritable;
import cs455.hadoop.airline.utils.SecurityCompositeKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecurityReducer extends Reducer<SecurityCompositeKey, MeanWritable, SecurityCompositeKey, Text> {
  @Override
  protected void reduce(SecurityCompositeKey key, Iterable<MeanWritable> values, Context context) throws IOException, InterruptedException {
    int count = 0;
    long sum = 0;
    for(MeanWritable value: values) {
      count += value.count;
      sum += value.sum;
    }

    double mean = (count != 0) ?
        (float)sum/count : 0;

    String out = String.format(
      "Flights: %d\tSecurity Delays: %d\tMean: %f", count, sum, mean
    );

    context.write(key, new Text(out));
  }
}
