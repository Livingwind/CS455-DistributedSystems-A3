package cs455.hadoop.airline.utils;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MeanWritable implements Writable {
  public Integer count = 0;
  public long sum = 0;

  public MeanWritable() {}

  public MeanWritable(int count, long sum) {
    this.count = count;
    this.sum = sum;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeVInt(dataOutput, count);
    WritableUtils.writeVLong(dataOutput, sum);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    count = WritableUtils.readVInt(dataInput);
    sum = WritableUtils.readVLong(dataInput);
  }

  @Override
  public String toString() {
    return count + "," + sum;
  }
}
