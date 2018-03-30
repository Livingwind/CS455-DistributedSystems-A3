package cs455.hadoop.airline.reducers;

import cs455.hadoop.airline.utils.CarrierWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CarrierReducer extends Reducer<Text, CarrierWritable, Text, Text> {
  @Override
  protected void reduce(Text key, Iterable<CarrierWritable> values, Context context)
      throws IOException, InterruptedException {
    String name = null;
    int count = 0;
    long sum = 0;

    for(CarrierWritable value: values) {
        if(value.desc == null) {
          count += value.count;
          sum += value.sum;
        } else {
          name = value.desc;
        }
    }

    if(count == 0) {
      return;
    }

    // There were no flights for this carrier but it was in the supplementary data
    double mean = (float) sum / count;

    String out = String.format(
        "Num delays: %d\tTotal Delays: %d\tMean: %.2f\tFullname: %s",
        count, sum, mean, name
      );

    context.write(
      new Text(key),
      new Text(out)
    );
  }
}
