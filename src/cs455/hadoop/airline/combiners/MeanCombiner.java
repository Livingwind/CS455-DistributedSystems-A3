package cs455.hadoop.airline.combiners;

import cs455.hadoop.airline.utils.DelayCompositeKey;
import cs455.hadoop.airline.utils.MeanWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MeanCombiner extends Reducer<DelayCompositeKey, MeanWritable, DelayCompositeKey, MeanWritable> {
  @Override
  protected void reduce(DelayCompositeKey key, Iterable<MeanWritable> values, Context context)
      throws IOException, InterruptedException {
    MeanWritable combined = new MeanWritable();
    for(MeanWritable value: values) {
      combined.count += value.count;
      combined.sum += value.sum;
    }

    context.write(key, combined);
  }
}
