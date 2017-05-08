
/**
 * TotalCount counts the total numbers of 
 * documents in the file
 * 
 * 
 * @author Ajaykumar Prathap
 * @since 2017-03-19
 */

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class TotalCount extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TotalCount.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TotalCount(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " MyMapper ");
      
      Configuration conf = new Configuration();
      String[] arg = new GenericOptionsParser(conf, args).getRemainingArgs();
      
      
      job.setJarByClass( this .getClass());
      
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      
      
      job.setMapperClass( TotalCountMapper .class);
      job.setReducerClass( TotalCountReducer .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class TotalCountMapper extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");
     // String pattern = '\[[(.*?)\]]\';
      private static final Pattern p = Pattern.compile("\\[[(.*?)\\]]");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

    	  String title = "";
    	  String text = "";
         String line  = lineText.toString();
         Text currentWord  = new Text();
         //emits for all lines
         if(!(line.isEmpty()) && line != null){
        	 context.write(new Text("N= "),one);
         }
         
       
      }
   }
   
   // Reducer counts all the number of lines in the document

   public static class TotalCountReducer extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
      @Override 
      public void reduce( Text key,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
         
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
         context.write(key,  new IntWritable(sum));
      }
   }
}
