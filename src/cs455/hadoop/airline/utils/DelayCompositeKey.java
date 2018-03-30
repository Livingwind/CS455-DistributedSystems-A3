package cs455.hadoop.airline.utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DelayCompositeKey implements WritableComparable<DelayCompositeKey> {
  public static final String TIME="TIME";
  public static final String DAY="DAY";
  public static final String MONTH="MONTH";

  /**
   * Time, Day, Month
   */
  String type;

  /** Logical breakup of the type
   *  0-23 for Time (of day)
   *  1-7 for Day (of the week)
   *  1-12 for Month
   */
  Integer partition;

  public DelayCompositeKey() {
  }

  public DelayCompositeKey(String type, int partition) {
    this.type = type;
    this.partition = partition;
  }

  @Override
  public int compareTo(DelayCompositeKey o) {
    if(o == null) {
      return 0;
    }

    int typeComp = type.compareTo(o.type);
    // If type is the same compare the partition, otherwise return the type comparison
    return typeComp != 0 ? typeComp : partition.compareTo(o.partition);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeString(dataOutput, type);
    WritableUtils.writeVInt(dataOutput, partition);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    type = WritableUtils.readString(dataInput);
    partition = WritableUtils.readVInt(dataInput);
  }

  @Override
  public String toString() {
    return type + ":" + partition;
  }
}
