import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

enum CustomCounters {NUM_ATHLETES}
public class supportAnalysis {

    public static void runJob(String[] input, String output) throws Exception {
         
        Job job = Job.getInstance(new Configuration());
        Configuration conf = job.getConfiguration();
        
        job.setJarByClass(supportAnalysis.class);
        job.setMapperClass(findatheletsnamesMapper.class);
        job.setReducerClass(countMentionsReducer.class);
  	  
        job.setOutputKeyClass(TextTextPair.class); //Key is (athelete_name Sport), type TextTextPair
        job.setOutputValueClass(IntWritable.class); // Value is one, type Intwritable with value 1
        
        job.addCacheFile(new Path("/data/medalistsrio.csv").toUri());
               
        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
        }
    
    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
        System.out.println("Job starting");
    }
}