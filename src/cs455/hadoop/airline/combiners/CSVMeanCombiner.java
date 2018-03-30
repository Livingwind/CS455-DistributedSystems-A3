package cs455.hadoop.airline.combiners;

import cs455.hadoop.airline.utils.MeanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CSVMeanCombiner extends Reducer<Text, MeanWritable, Text, MeanWritable> {

  @Override
  protected void reduce(Text key, Iterable<MeanWritable> values, Context context) throws IOException, InterruptedException {
    MeanWritable combined = new MeanWritable();
    for(MeanWritable value: values) {
      combined.count += value.count;
      combined.sum += value.sum;
    }

    context.write(new Text(key.toString()+','), combined);
  }
}
