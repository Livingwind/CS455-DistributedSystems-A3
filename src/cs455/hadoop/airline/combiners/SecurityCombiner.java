package cs455.hadoop.airline.combiners;

import cs455.hadoop.airline.utils.MeanWritable;
import cs455.hadoop.airline.utils.SecurityCompositeKey;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecurityCombiner extends Reducer<SecurityCompositeKey, MeanWritable, SecurityCompositeKey, MeanWritable> {

  @Override
  protected void reduce(SecurityCompositeKey key, Iterable<MeanWritable> values, Context context)
      throws IOException, InterruptedException {
    int count = 0;
    long delay = 0;
    for(MeanWritable value: values) {
      count += value.count;
      delay += value.sum;
    }
    context.write(key, new MeanWritable(count, delay));
  }
}
