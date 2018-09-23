import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import org.apache.commons.lang.StringUtils;


public class findatheletsnamesMapper extends Mapper<Object, Text, TextTextPair, IntWritable>  {

	private Hashtable<String, String> athletesInfo;  //hashtable with:   athletes name  | sport

	private TextTextPair name_sport = new TextTextPair();
	private final IntWritable one = new IntWritable(1);


	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//********************************************
		//input: one line of the Twitter database of the type Text.
		//output: a key-value pair: key = name_Sport type TextTextPair, value = one type IntWritable with value 1.
		//This method processes the input to fill a string vector with the different fields accordingly to the structure of a tweet given in the guidelines.
		//from the message field instances of each athlete name stored in a hashtable are found.
		//for each match a key -value pair is emitted
		//Tweets that do not follow the structure were filtered.
		//********************************************

		String line;
		String[] fields;
		String tweet;
		String text;
		line = value.toString();
		fields =line.split(";");
		String sport;


		//Count Number of fields, only proceed if equal to 4!!!
		if (fields.length ==4){

			text = fields[2];  // text is a string with all our message!
			tweet=text.toLowerCase(); // transform text to lowerCase in order to get more matches for athletes names.
			
			//********************************************************************************************************
			// run through names in hashtable and try to match each name to a substring in the tweet.
			// if a name is found-> emit a key value pair of the type: ([name sport];1)
			//*********************************************************************************************
			for (Enumeration<String> e = athletesInfo.keys(); e.hasMoreElements();){
				String UCname = e.nextElement();  // get the athlete's name from the hashtable.
				String name= UCname.toLowerCase(); // athlete name in lower case to use as substring to find in tweet message.
				//System.out.println(name+":");

				int n_matches = StringUtils.countMatches(tweet, name); //find how many times the athlete name in lower case appears in the tweet message.
				//System.out.println("number of matches of " +name+" :"+n_matches);
			
				for( int i = 1; i <= n_matches; i++ ){ //for each match found...

					sport = athletesInfo.get(UCname); //get the sport from the hashtable.

//					System.out.println(name+" "+ sport +" "+ i +" of " + n_matches); 
	//				if( sport.isEmpty()){
		//				sport="cococococococco";
//					}
					//	try {
					name_sport.set(UCname, sport);  // set the key to original name sport
					//}catch(Exception e3) {
					//System.err.println("o ERRO está na escrita da key no mapper:  ->"+ e3.toString());
					//System.err.println(name_sport.toString());
					//}
					context.write(name_sport, one); // emit a key-value pair.

				}

			}
		} 
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		athletesInfo = new Hashtable<String, String>();
		//
		//		// We know there is only one cache file, so we only retrieve that URI
		URI fileUri = context.getCacheFiles()[0]; // get medalistrio table from cache.

		//
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));
		//
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		//

		String line = null;
		try {
			//			// we discard the header row
			br.readLine();
			//
			while ((line = br.readLine()) != null) {

				//context.getCounter(CustomCounters.NUM_ATHLETES).increment(1);
				
				String[] campo = line.split(","); //split one row of the table into fields:

				// Fields are: 0:ID 1:Name 2: 3: 4: 5: 6: 7:Sport, 8: 9: 10: 
				if (campo.length == 11)
					athletesInfo.put(campo[1], campo[7]);// insert row to hashtable with name as key and sport as value.
					//System.out.println(" hash filled out with " + fields[1]+" -->" +athletesInfo.get(fields[1]));
			}
			br.close();
		} catch (IOException e1) {
			System.err.println("ERRO NO PROCESSAMENTO DO HASHTABLE:  "+ e1.toString());
		}
		super.setup(context);
	}


}
