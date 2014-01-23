/*
 * 
 * INLS 690 final project Q3 - calculate commonality between any two studies by people involved into the studies
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

public class PeopleComp{

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: PeopleComp <input path> <output path>");
      System.exit(-1);
    }
    
    JobConf conf = new JobConf(PeopleComp.class);
    conf.setJobName("Abstract Comparison");

    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setNumReduceTasks(60);
    conf.setMapperClass(PeopleCompMapper.class);
    conf.setReducerClass(PeopleCompReducer.class);
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    JobClient.runJob(conf);
  }
}

