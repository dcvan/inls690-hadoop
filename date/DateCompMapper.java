/*
 * 
 * INLS 690 final project Q4 - calculate commonality between any two studies by time period
 * 
 * This program is a mapper program. It is used for capturing ID and time peirod of every study
 *
 * @author Fan Jiang
 *
 */

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DateCompMapper extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, Text> {
  //study id pattern
  private static final String ID_PAT = "<IDNo agency=\"handle\">([^<>]*)</IDNo>";
  //time period pattern
  private static final String DA_PAT = "<timePrd [^>]*>([^<>]*)</timePrd>";

  public void map(LongWritable key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
    //read a line from the input
    String line = value.toString();
   
    //new matchers for study id and time period
    Pattern idPattern = Pattern.compile(ID_PAT);
    Pattern datePattern = Pattern.compile(DA_PAT);
    Matcher idMatcher = idPattern.matcher(line); 
    Matcher dateMatcher = datePattern.matcher(line);
    
    //find study id;return if study id not found(i.e. the reading line is not a study record)
    String id = null;
    if(idMatcher.find()) id = idMatcher.group(1);
    else return; 
   
    while(dateMatcher.find()){
	String date = dateMatcher.group(1);
	//format the date into "yyyy-mm-dd" to keep consistency
	if(date.split("-").length == 1)
		date += "-01-01";
	else if(date.split("-").length == 2)
		date += "-01";
        //remove dashes to make the date a comparable integer, saving convertion from string to date in pig
	date = date.replaceAll("-", "");
	output.collect(new Text(id), new Text(date)); 
    }
  }
}
