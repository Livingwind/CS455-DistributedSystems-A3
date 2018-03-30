package cs455.hadoop.airline.reducers;

import cs455.hadoop.airline.utils.TopWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class TopReducer extends Reducer<Text, TopWritable, Text, Text> {
  private TreeMap<String, Integer> parseHubs(Iterable<TopWritable> values) {
    TreeMap<String, Integer> map = new TreeMap<>();
    for(TopWritable value: values) {
      Integer x = map.containsKey(value.hub) ?
          map.put(value.hub,(map.get(value.hub) + value.count)) :
          map.put(value.hub, value.count);
    }

    return map;
  }

  private ArrayList<String> busiest(TreeMap<String, Integer> map, int bound) {
    TreeSet<Map.Entry<String, Integer>> sorted = new TreeSet<>(
      new Comparator<Map.Entry<String, Integer>>() {
        @Override
        public int compare(Map.Entry<String, Integer> first, Map.Entry<String, Integer> second) {
          int comp = first.getValue().compareTo(second.getValue());
          return comp == 0 ? 1 : comp;
        }
      }
    );

    sorted.addAll(map.entrySet());
    ArrayList<String> top = new ArrayList<>();
    for(int i = 0; i < bound; i++) {
      Map.Entry<String, Integer> next = sorted.pollLast();
      if(next != null) {
        top.add(next.getKey());
      }
    }
    return top;
  }


  @Override
  protected void reduce(Text key, Iterable<TopWritable> values, Context context)
      throws IOException, InterruptedException {
    TreeMap<String, Integer> map = parseHubs(values);
    StringBuilder sb = new StringBuilder();
    for(String hub: busiest(map, 10)) {
      sb.append(hub + " ");
    }

    context.write(key, new Text(sb.toString()));
  }
}
