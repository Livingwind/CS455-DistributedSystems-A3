package cs455.hadoop.airline.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HadoopUtils {
  /**
   * Clears the DFS directory with the given path and returns a HDFS Path object
   * @param conf
   * @param path
   * @return the cleared Path
   * @throws IOException
   */
  public static Path clearPath(Configuration conf, String path)
      throws IOException {
    Path tmpPath = new Path(path);
    FileSystem fs = FileSystem.get(conf);
    if(fs.exists(tmpPath)) {
      fs.delete(tmpPath, true);
      System.out.println("INFO: Cleared path " + path + " in the DFS.");
    }
    return tmpPath;
  }
}
