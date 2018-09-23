import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.StringTokenizer;



public class putinbinsMapper extends Mapper<Object, Text, IntWritable, IntWritable>  {

	private final IntWritable one = new IntWritable(1);
	private final IntWritable binID = new IntWritable(0);

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//********************************************
		//input: one line of the Twitter database of the type Text.
		//output: a key-value pair: key = binID type IntWritable, value = one type IntWritable with value 1.
		//This method processes the input to fill a string vector with the different fields accordingly to the structure of a tweet given in the guidelines.
		//the message field is then used to compute message length, which is used to compute the BinID.
		//Tweets that do not follow the structure were filtered out as well as messages which length is higher than the imposed limit by Twitter at the time of the Olympic Games( 140 characters).
		//********************************************
		String line;
		String[] fields;
		String tweet;


		line = value.toString(); //Read Tweet from input to String line.
		fields =line.split(";"); //separate line in different fields.

		//Count Number of fields, only proceed if equal to 4!!!
		if (fields.length ==4){

			tweet = fields[2];  // tweet is a string with the message!

			int mlength = tweet.length(); //message length computation
			if (mlength < 140){ //probably strange characters, tweet should be thrown away since it will create bad results.
				
				// Bin Identification:				
				binID.set(((mlength-1)/5)) ; // from 0 to 27 -Works since (/) is an integer division :  1-5, 6-10, ...... 136-140 = 28 bins
				
				context.write(binID, one); //emit the key-value pair
			}else {

				System.out.println("message length higher than 140---> not producing a key, value pair");
			}

		} else {
			System.out.println("Number of fields different than 4!---> Not producing a key-value Pair");
		}
		// *********************************************




	}
}
