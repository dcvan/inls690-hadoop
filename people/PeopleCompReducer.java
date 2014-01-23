/*
 * 
 * INLS 690 final project Q3 - calculate commonality between any two studies by people involved into the studies. 
 * 
 * This program is a reducer program. It is used for calculating the total number of words in a document.
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

public class PeopleCompReducer extends MapReduceBase
  implements Reducer<Text, Text, Text, Text> {

  public void reduce(Text key, Iterator<Text> values,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
	while (values.hasNext()) 
        	output.collect(key, new Text(values.next()));
  }
}
