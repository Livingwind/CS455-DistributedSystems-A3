package cs455.hadoop.airline.utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HubsCompositeKey implements WritableComparable<HubsCompositeKey> {
  String year;
  String hub;

  public HubsCompositeKey() {}

  public HubsCompositeKey(String year, String hub) {
    this.year = year;
    this.hub = hub;
  }

  @Override
  public int compareTo(HubsCompositeKey o) {
    if(o == null) {
      return 0;
    }

    int comp = year.compareTo(o.year);
    return comp != 0 ? comp : hub.compareTo(o.hub);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeString(dataOutput, year);
    WritableUtils.writeString(dataOutput, hub);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    year = WritableUtils.readString(dataInput);
    hub = WritableUtils.readString(dataInput);
  }

  @Override
  public String toString() {
    return year + ":" + hub;
  }
}
