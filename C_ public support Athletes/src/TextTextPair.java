import java.io.*;
import org.apache.hadoop.io.*;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextTextPair implements WritableComparable<TextTextPair> {

   private Text first;
   private Text second;

   public TextTextPair() {
		first = new Text();
		second = new Text();
	}

   public TextTextPair(String one, String two) {
	   
	   set(one, two);
   }

   public void set(String one, String two) {
     
   Text coco1 = new Text(one);
   Text coco2 = new Text(two);
   first=coco1;
   second=coco2;
   //first.set(new Text(one));
     //second.set(new Text(two));
   }

   public Text getFirst() {
      return first;
   }

   public Text getSecond() {
      return second;
   }

   @Override
   public void write(DataOutput out) throws IOException {
      first.write(out);
      second.write(out);
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      first.readFields(in);
      second.readFields(in);
   }

   @Override
   public int hashCode() {
      return first.hashCode() * 163 + second.hashCode();
   }

   @Override
   public boolean equals(Object o) {
      if (o instanceof TextTextPair) {
         TextTextPair tp = (TextTextPair) o;
         return first.equals(tp.first) && second.equals(tp.second);
      }
      return false;
   }

   @Override
   public String toString() {
      return first.toString() + "\t" + second.toString();
   }

   @Override
   public int compareTo(TextTextPair tp) {
      int cmp = first.compareTo(tp.first);
      if (cmp != 0) {
         return cmp;
      }
      return second.compareTo(tp.second);
   }
}
