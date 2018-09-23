import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class getHashtagsMapper extends Mapper<Object, Text, Text, IntWritable>  {

	private final IntWritable one = new IntWritable(1); //value
	private final Text hashtag = new Text(); //key

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//********************************************
		//input: one line of the Twitter database of the type Text.
		//output: a key-value pair: key = hashtag type Text, value = one, type IntWritable with value 1.
		//This method processes the input to fill a string vector with the different fields accordingly to the structure of a tweet given in the guidelines.
		//The time field which is an epoch string is used to compute the hourTweet (hour at which the tweet was sent), if hourTweet is the same as the busiest hour the method will procede to identify hashtags. the busiest hour is hardcoded into the Int hour..
		//for each hashtag found this method will produce a ky-value pair of the form (hashtag, 1)
		//
		//********************************************
		String line;
		String[] fields;
		String tweet;
		int hour = 2; 	//hardcoded from previous question assignment partB 2 UTC -> 23 Brasil time.  15UTC-> 12pm, 21UTC--> 18pm
		int hourTweet = 0;

		line = value.toString();//transform input to string type
		fields =line.split(";");//splits string line and fill out fields array

		//Count Number of fields, only proceed if equal to 4!!!
		if (fields.length ==4){
			if (StringUtils.isNumeric(fields[0])) { // if field 0 is not numeric, it is not an epoch time stamp and shouldn't be considered.
				
				Date tweetTimeStamp= new Date(Long.parseLong(fields[0]));// from epoch get a date object.
				hourTweet =tweetTimeStamp.getHours(); // get hour from the Date object.
				int month =tweetTimeStamp.getMonth(); // get month from the Date object.
				int day = tweetTimeStamp.getDay();  // get day from the Date object.
				
				
				//*********** Uncomment to filter out tweets sent outside event days! ***********************
				//if (month == 7  && day < 21 && day > 5) {  //filter out tweet not during event, MEs 7--> É agosto!!!
				//********************************************************************************************	
					if (hourTweet==hour) { //----> proceed only if hour corresponds to x

						String text = fields[2]; // tweet is a string with all our message!
						//process tweet to get hashtags:
						text=text.toLowerCase();  //transform message to lower case-> hashtags with different case letter will be considered the same!
						String space = " ";  
						tweet= space.concat(text); //introduce a space in the beginning of the message to be able to retrieve hashtags in the beginning.


						Pattern p = Pattern.compile("[\\s\\W]#(\\w+)"); //regex pattern no get the word that is following the symbol#
						Matcher m = p.matcher(tweet); //matches pattern to message string in order to find the hashtags,
						int n_groups= m.groupCount();
						//	System.out.println(n_groups);

						while(m.find()){  //for each hashtag found emit a key, value pair! this will loop until no more hashtags were found.
							//System.out.println(m.group(1));
							hashtag.set(m.group(1)); //define the KEY with the hashtag found
							context.write(hashtag, one); //emit the key-value pair.
						}																	
					}
				}
			}
		}
	}
}