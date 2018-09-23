import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class countocurrencesReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
  
	private IntWritable result = new IntWritable();
	
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
    		throws IOException, InterruptedException {
	//***************************************************************
	//input: a pair of the form [key, values]  key is of the type IntWritable and values is a vector of IntWritables each with the value 1
	//output: the pair [key, result] , result is of the type IntWritable.
	//method: this method increases a counter by one for each element in the vector values and sets a result variable with the final value of this counter. 
	//*****************************************************************************************
	
	
	
    	int count = 0;

        for (IntWritable value : values) {
        	count=count+1;
        	//=value.get();
        }

        result.set(count);
        context.write(key, result);
    }
}