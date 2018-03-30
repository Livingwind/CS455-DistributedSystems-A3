package cs455.hadoop.airline.mappers;

import cs455.hadoop.airline.utils.CarrierWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CarrierSuppMapper extends Mapper<LongWritable, Text, Text, CarrierWritable> {
  private ArrayList<String> parseCarriers(String value) {
    Pattern pattern = Pattern.compile("\"(.*?)\"");
    Matcher matcher = pattern.matcher(value);

    ArrayList<String> info = new ArrayList<>();
    while(matcher.find()) {
      info.add(matcher.group(1));
    }

    return info;
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    ArrayList<String> info = parseCarriers(value.toString());
    if(info.isEmpty()) {
      return;
    }

    String code = info.get(0);
    String desc = info.get(1);

    context.write(new Text(code), new CarrierWritable(desc));
  }
}
