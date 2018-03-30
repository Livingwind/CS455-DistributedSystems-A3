package cs455.hadoop.airline.combiners;

import cs455.hadoop.airline.utils.AgeWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class AgeCombiner extends Reducer<Text, AgeWritable, Text, AgeWritable> {
  @Override
  protected void reduce(Text key, Iterable<AgeWritable> values, Context context) throws IOException, InterruptedException {
    HashMap<Integer, AgeWritable> map = new HashMap<>();
    for(AgeWritable value: values) {
      // If year == 0 it means this is an age association so we just write it
      if(value.year == 0) {
        context.write(key, value);
        continue;
      }

      if(map.containsKey(value.year)) {
        AgeWritable e = map.get(value.year);
        e.count += value.count;
        e.delay += value.delay;
      } else {
        map.put(value.year, new AgeWritable(value));
      }
    }

    for(AgeWritable value: map.values()) {
      context.write(key, value);
    }
  }
}