/*
 * 
 * INLS 690 final project Q1 - calculate commonality between any two studies by keywords.
 * 
 * This program is a reducer program. It is used for writing "id keyword" pairs
 * to the output. 
 *
 * @author Fan Jiang
 *
 */

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class KeywordCompReducer extends MapReduceBase
  implements Reducer<Text, Text, Text, Text> {

  public void reduce(Text key, Iterator<Text> values,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
	while (values.hasNext()) 
        	output.collect(key, new Text(values.next()));
  }
}
