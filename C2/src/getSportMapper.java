import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class getSportMapper extends Mapper<Object, Text, Text, IntWritable>  {

	private final IntWritable inter_mentions = new IntWritable();
	private final Text Sport = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//*******************************************************
		//input: one line of the file with the structure: Name	Sport	Number of mentions  
		//output: a key-value pair: key = Sport type Text, value = number of mentions type IntWritable
		//This method processes the input to fill a string vector with the different fields accordingly to the input's structure mentioned.
		//the key is set with the value of the second field (sport) and the value is set with the value of the 3rd field (number of mentions).
		//for each line in the input a key-value pair is emitted
		//********************************************
 
		String line;
		line = value.toString();  //transforms the input to a String and assigns it to the variable line.
		
		String[] fields =line.split("\t");//split the string line into fields, the delimitter between fields is Tab.
		Sport.set(fields[1]); //the key is set with the sport.
		
		inter_mentions.set(Integer.parseInt(fields[2]));  // value is set with the number of mentions 
		
		context.write(Sport, inter_mentions);// emit key, value pair of the kind: Sport, #mentions.
	
	}
}
