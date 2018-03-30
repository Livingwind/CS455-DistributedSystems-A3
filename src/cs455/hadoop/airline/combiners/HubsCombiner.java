package cs455.hadoop.airline.combiners;

import cs455.hadoop.airline.utils.HubsCompositeKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HubsCombiner extends Reducer<HubsCompositeKey, IntWritable, HubsCompositeKey, IntWritable> {
  @Override
  protected void reduce(HubsCompositeKey key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int count = 0;
    for(IntWritable value: values) {
      count += value.get();
    }

    context.write(key, new IntWritable(count));
  }
}
