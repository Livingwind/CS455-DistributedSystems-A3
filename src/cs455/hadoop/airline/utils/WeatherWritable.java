package cs455.hadoop.airline.utils;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WeatherWritable implements Writable {
  public String city;
  public int count;
  public long sum;

  public WeatherWritable() {}

  public WeatherWritable(String city) {
    this.city = city;
  }

  public WeatherWritable(int count, long sum) {
    this.count = count;
    this.sum = sum;
  }

  public WeatherWritable(String city, int count, long sum) {
    this.city = city;
    this.count = count;
    this.sum = sum;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeString(dataOutput, city);
    WritableUtils.writeVInt(dataOutput, count);
    WritableUtils.writeVLong(dataOutput, sum);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    city = WritableUtils.readString(dataInput);
    count = WritableUtils.readVInt(dataInput);
    sum = WritableUtils.readVLong(dataInput);
  }

  @Override
  public String toString() {
    return city + " " + count + " " + sum;
  }
}
