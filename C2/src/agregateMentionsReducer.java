import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import org.apache.commons.lang.StringUtils;


public class agregateMentionsReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	  
		private IntWritable total_mentions = new IntWritable();
		
	    public void reduce(Text key, Iterable<IntWritable> values, Context context)
	    		throws IOException, InterruptedException {
	//***************************************************************
	//input: a pair of the form [key, values]  key is of the type Text and values is a vector of IntWritables.
	//output: the pair [key, total_mentions], total_mentions is of the type IntWritable.
	//method: this method sums all elements of the values list and sets the total_mentions variable with the final value of this counter. 
	//*****************************************************************************************
		
	    	int count = 0;

	        for (IntWritable value : values) {
	        	count+=value.get();
	        }

	        total_mentions.set(count);
	        context.write(key, total_mentions);
}
	    }
