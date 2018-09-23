
import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class countMentionsReducer extends Reducer<TextTextPair, IntWritable, TextTextPair, IntWritable>{
	  
		private IntWritable result = new IntWritable();
		
	    public void reduce(TextTextPair key, Iterable<IntWritable> values, Context context)
	    		throws IOException, InterruptedException {



	    	int count = 0;

	        for (IntWritable value : values) {
	        	count+=value.get();
	        }

	        result.set(count);
	        context.write(key, result);
	    }
}