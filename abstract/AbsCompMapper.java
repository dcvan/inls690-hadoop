/*
 * 
 * INLS 690 final project Q1 - calculate commonality between any two studies by abstract
 * 
 * This program is a mapper program. It is used for capturing ID and abstract of every study.
 *
 * @author Fan Jiang
 *
 */

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AbsCompMapper extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, Text> {
  //study id pattern
  private static final String ID_PAT = "<IDNo agency=\"handle\">([^<>]*)</IDNo>";
  //abstract pattern
  private static final String AB_PAT = "<abstract[^>]*>([^<>]*)</abstract>";

  public void map(LongWritable key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
    //read a line from the input
    String line = value.toString();
    
    //new matchers for id and abstract
    Pattern idPattern = Pattern.compile(ID_PAT);
    Pattern absPattern = Pattern.compile(AB_PAT);
    Matcher idMatcher = idPattern.matcher(line); 
    Matcher absMatcher = absPattern.matcher(line);
    
    //find study id; return if study id not found(i.e. the reading line is not a study record). 
    String id = null;
    if(idMatcher.find()) id = idMatcher.group(1);
    else return; 
    
    //find abstract
    while(absMatcher.find()){
        //remove garbage and tokenize the text by space(s)
	String[] matched = absMatcher.group(1)
		.toLowerCase()
		.replaceAll("&lt;", " ")
		.replaceAll("&gt;", " ")
		.replaceAll("br /"," ")
		.replaceAll("[‘’]", "'")
		.replaceAll(" '", "")
		.replaceAll("' ", "")
		.replaceAll("[`.,:;/\")(]", " ")
		.split(" +");

        //remove duplicate words
	Set<String> vocab = new HashSet<String>();
	vocab.addAll(Arrays.asList(matched));
	
	//output "id word" pairs iteratively
        for(String word : vocab)	
		if(!word.isEmpty())
			output.collect(new Text(id), new Text(word.trim())); 
    }
  }
}
