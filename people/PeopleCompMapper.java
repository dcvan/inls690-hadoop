/*
 * 
 * INLS 690 final project Q3 - calculate commonality bewteen any two studies by people involved in the studies
 * 
 * This program is a mapper program. It is used for capturing ID and people of every study
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

public class PeopleCompMapper extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, Text> {
  //study id pattern
  private static final String ID_PAT = "<IDNo agency=\"handle\">([^<>]*)</IDNo>";
  //people pattern
  private static final String PP_PAT = "<AuthEnty[^>]*>([^<>]*)</AuthEnty>";

  public void map(LongWritable key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException { 
    //read a line from input 
    String line = value.toString();
    
    //new matchers for study id and people involved in the study
    Pattern idPattern = Pattern.compile(ID_PAT);
    Pattern peoplePattern = Pattern.compile(PP_PAT);
    Matcher idMatcher = idPattern.matcher(line); 
    Matcher peopleMatcher = peoplePattern.matcher(line);

    //find study id;return if id not found(i.e. the reading line is not a study record)
    String id = null;
    if(idMatcher.find()) id = idMatcher.group(1);
    else return; 

    while(peopleMatcher.find()){
	//Downcase and remove studies with anonymous participants
	String group = peopleMatcher.group(1).toLowerCase().replaceAll("n/a", "");

        //output "id people" pairs iteratively
	if(!group.isEmpty())
		output.collect(new Text(id), new Text(group.trim())); 
    }
  }
}
