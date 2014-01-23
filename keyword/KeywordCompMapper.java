/*
 * 
 * INLS 690 final project Q1 - calculate commonality between any two studies by keywords. 
 * 
 * This program is a mapper program. It is used for capturing ID and keywords of every study.
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

public class KeywordCompMapper extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, Text> {
  //study id pattern
  private static final String ID_PAT = "<IDNo agency=\"handle\">([^<>]*)</IDNo>";
  //keyword pattern
  private static final String KW_PAT = "<keyword[^>]*>([^<>]*)</keyword>";

  public void map(LongWritable key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
    //read a line from the input
    String line = value.toString();
    
    //new matchers for id and keywords
    Pattern idPattern = Pattern.compile(ID_PAT);
    Pattern keywordPattern = Pattern.compile(KW_PAT);
    Matcher idMatcher = idPattern.matcher(line); 
    Matcher keywordMatcher = keywordPattern.matcher(line);
    
    //find study id; return if study id not found(i.e. the reading line is not a study record). 
    String id = null;
    if(idMatcher.find()) id = idMatcher.group(1);
    else return; 
    
    //find keywords
    while(keywordMatcher.find()){
        //remove garbage and attempt to tokenize the text by a comma or a semicolon
	String[] matched = keywordMatcher.group(1)
		.toLowerCase()
		.replaceAll("&lt;", ";")
		.replaceAll("br/&gt;", "")
		.split("[,;]");

        //remove duplicate keywords
	Set<String> keywords = new HashSet<String>();
	keywords.addAll(Arrays.asList(matched));
	
	//output "id keyword" pairs iteratively
        for(String word : keywords)	
		output.collect(new Text(id), new Text(word.trim())); 
    }
  }
}
