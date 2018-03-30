package cs455.hadoop.airline.utils;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopWritable implements Writable {
  public String hub;
  public int count;

  public TopWritable() {}

  public TopWritable(String hub, int count) {
    this.hub = hub;
    this.count = count;
  }

  public TopWritable(TopWritable o) {
    hub = o.hub;
    count = o.count;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeString(dataOutput, hub);
    WritableUtils.writeVInt(dataOutput, count);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    hub = WritableUtils.readString(dataInput);
    count = WritableUtils.readVInt(dataInput);
  }

  @Override
  public String toString() {
    return hub + ' ' + count;
  }
}
