package cs455.hadoop.airline.utils;

import com.sun.corba.se.spi.ior.Writeable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CarrierWritable implements Writable {
  public String desc = null;
  public int count;
  public long sum;

  public CarrierWritable() {}

  public CarrierWritable(int count, long sum) {
    this.count = count;
    this.sum = sum;
  }

  public CarrierWritable(String desc) {
    this.desc = desc;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
      WritableUtils.writeVInt(dataOutput, count);
      WritableUtils.writeVLong(dataOutput, sum);
      WritableUtils.writeString(dataOutput, desc);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
      count = WritableUtils.readVInt(dataInput);
      sum = WritableUtils.readVLong(dataInput);
      desc = WritableUtils.readString(dataInput);
  }

  @Override
  public String toString() {
    return "Fullname: " + desc + "\tCount: " + count + "\tSum:" + sum;
  }
}
