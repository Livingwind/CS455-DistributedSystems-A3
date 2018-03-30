package cs455.hadoop.airline.reducers;

import cs455.hadoop.airline.utils.AgeWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class AgeInterReducer extends Reducer<Text, AgeWritable, Text, Text> {
  @Override
  protected void reduce(Text key, Iterable<AgeWritable> values, Context context)
      throws IOException, InterruptedException {
    ArrayList<AgeWritable> list = new ArrayList<>();
    int manYear = 0;
    for(AgeWritable value: values) {
      if(value.age == 0) {
        list.add(new AgeWritable(value));
      } else{
        manYear = value.age;
      }
    }

    if(manYear == 0) {
      return;
    }

    for(AgeWritable value: list) {
      if(value.year == 0) {
        continue;
      }
      String out = String.format(
          "%d %d %d",
          value.year - manYear, value.count, value.delay
      );

      context.write(key, new Text(out));
    }
  }
}
