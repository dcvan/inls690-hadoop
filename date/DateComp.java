/*
 * 
 * INLS 690 final project Q4 - calculate commonality between any two studies by time period 
 * 
 * This program is a job configuration program.
 *
 * @author Fan Jiang
 *
 */

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class DateComp{

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: DateComp <input path> <output path>");
      System.exit(-1);
    }
    
    JobConf conf = new JobConf(DateComp.class);
    conf.setJobName("Abstract Comparison");

    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setNumReduceTasks(60);
    conf.setMapperClass(DateCompMapper.class);
    conf.setReducerClass(DateCompReducer.class);
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    JobClient.runJob(conf);
  }
}

