package mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import javax.naming.Context;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class PivotTranslation {

  public static class Map extends Mapper<Object, Text, Text, IntWritable>{
      
  }
    private Text word = new Text();
    private int i = 0;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      
      StringTokenizer itr = new StringTokenizer(value.toString(), ";");
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, i);
        i++;
      }
    }
  

  public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
      
    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
       String txt = "";
       Iterator<IntWritable> itr = values.iterator();
       boolean first = true;
        while (itr.hasNext()) {
            if(!first) 
                txt += itr.next() + ";";
            else 
                txt+= itr.next(); 
        }
        context.write(key, txt);
    }
  }}