
public class getHourMapper extends Mapper<Object, Text, IntWritable, IntWritable>  {

	private final IntWritable one = new IntWritable(1);// value
	private final IntWritable binID = new IntWritable(0);  //key

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//********************************************
		//input: one line of the Twitter database of the type Text.
		//output: a key-value pair: key = binID type IntWritable, value = one type IntWritable with value 1.
		//This method processes the input to fill a string vector with the different fields accordingly to the structure of a tweet given in the guidelines.
		//The time field which is an epoch string is then used to compute the hour at which the tweet was sent, this hour is the value of the key.
		//Tweets that do not follow the structure were filtered.
		//********************************************
	
		String line;
		String[] fields = null;
		String tweet;

		line = value.toString();  //transform input to string type
		fields =line.split(";");	//splits string line and fill out fields array

		//try {
		if (fields.length == 4){//Continue only if there are 4 fields
		
			if (StringUtils.isNumeric(fields[0])) {  // if field 0 is not numeric, it is not an epoch time stamp and shouldn't be considered.

				Date tweetTimeStamp= new Date(Long.parseLong(fields[0]));// from epoch get a date object.

				int month =tweetTimeStamp.getMonth(); // get month from the Date object.
				int day = tweetTimeStamp.getDay(); //get the day from the Date object.

				//*********** Uncomment to filter out tweets sent outside event days! ***********************
				//if (month == 7  && day < 21 && day > 5) {  //filter out tweet not during event, MEs 7--> É agosto!!!
				//********************************************************************************************	
					int hour =tweetTimeStamp.getHours();//get hour from Date object
					binID.set(hour) ;   // set key with value of hour 
					
					
					context.write(binID, one); // emit key-value pair
				//*******************************************************************************************	
				//}
				//********************************************************************************************
			}
		}
		//	} catch (Exception e) {

		//System.err.println(" A Excepção é ::::::"+ e.getMessage());
		//System.out.println(line);
		//System.out.println(fields[0]);



		//}	

	}
}