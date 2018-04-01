package cs455.hadoop.airline.utils;

import cs455.hadoop.airline.enums.Month;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecurityCompositeKey implements WritableComparable<SecurityCompositeKey> {
  public Integer year;
  public Month month;

  public SecurityCompositeKey() {}

  public SecurityCompositeKey(int year, Month month) {
    this.year = year;
    this.month = month;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeVInt(dataOutput, year);
    WritableUtils.writeVInt(dataOutput, month.value);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    year = WritableUtils.readVInt(dataInput);
    month = Month.value(WritableUtils.readVInt(dataInput));
  }

  @Override
  public int compareTo(SecurityCompositeKey o) {
    if(o == null) {
      return -1;
    }
    int yearComp = year.compareTo(o.year);
    return yearComp != 0 ? yearComp: month.compareTo(o.month);
  }

  @Override
  public String toString() {
    return year + "-" + month + ":";
  }
}
