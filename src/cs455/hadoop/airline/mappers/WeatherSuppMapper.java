package cs455.hadoop.airline.mappers;

import cs455.hadoop.airline.utils.WeatherWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WeatherSuppMapper extends Mapper<LongWritable, Text, Text, WeatherWritable> {
  private ArrayList<String> parseWeather(String value) {
    Pattern pattern = Pattern.compile("(([^\"][^,]*)|\"(.*?)\"),?");
    Matcher matcher = pattern.matcher(value);

    ArrayList<String> info = new ArrayList<>();
    while(matcher.find()) {
      info.add(matcher.group(1));
    }

    return info;
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    ArrayList<String> entries = parseWeather(value.toString());

    String code = entries.get(0).replace("\"", "");
    String city = entries.get(2).replace("\"", "");

    context.write(new Text(code), new WeatherWritable(city));
  }
}
