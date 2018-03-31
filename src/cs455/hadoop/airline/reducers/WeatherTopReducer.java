package cs455.hadoop.airline.reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class WeatherTopReducer extends Reducer<Text, Text, Text, Text> {

  private TreeMap<String, Float> collectResults(Iterable<Text> values) {
    TreeMap<String, Float> map = new TreeMap<>();
    for(Text value: values) {
      String[] splits = value.toString().split(",");
      if(splits.length == 2) {
        map.put(splits[0].trim(), Float.parseFloat(splits[1]));
      }
    }

    return map;
  }

  public ArrayList<String> mostDelay(TreeMap<String, Float> map)
      throws IOException, InterruptedException {
    TreeSet<Map.Entry<String, Float>> sorted = new TreeSet<>(
      new Comparator<Map.Entry<String, Float>>() {
        @Override
        public int compare(Map.Entry<String, Float> first, Map.Entry<String, Float> second) {
          int comp = first.getValue().compareTo(second.getValue());
          return comp == 0 ? 1 : comp;
        }
      }
    );

    sorted.addAll(map.entrySet());
    ArrayList<String> top = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Map.Entry<String, Float> next = sorted.pollLast();
      if (next != null) {
        top.add(next.getKey() + "(" + next.getValue() + ")");
      }
    }
    return top;
  }

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    ArrayList<String> results =  mostDelay(collectResults(values));
    StringBuilder sb = new StringBuilder();
    for(int i = 0; i < 10; i++) {
      sb.append(i+1 + "." + results.get(i) + " ");
    }

    context.write(new Text(sb.toString()), new Text());
  }
}
