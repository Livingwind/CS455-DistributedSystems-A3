package cs455.hadoop.airline.utils;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AgeWritable implements Writable {
  public int age;
  public int year;
  public int count;
  public long delay;

  public AgeWritable() {}

  public AgeWritable(int age) {
    this.age = age;
  }

  public AgeWritable(int year, int count, long delay) {
    this.age = 0;
    this.year = year;
    this.count = count;
    this.delay = delay;
  }
  public AgeWritable(AgeWritable o) {
    this.age = o.age;
    this.year = o.year;
    this.count = o.count;
    this.delay = o.delay;
  }
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeVInt(dataOutput, age);
    WritableUtils.writeVInt(dataOutput, year);
    WritableUtils.writeVInt(dataOutput, count);
    WritableUtils.writeVLong(dataOutput, delay);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    age = WritableUtils.readVInt(dataInput);
    year = WritableUtils.readVInt(dataInput);
    count = WritableUtils.readVInt(dataInput);
    delay = WritableUtils.readVLong(dataInput);
  }

  @Override
  public String toString() {
    return year + " " + age + " " + delay;
  }
}
