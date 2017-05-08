

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.cloud.InvertedIndex.InvertedIndexReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Sorter has both mapper and reducer to sort
 * the title in descending order
 * 
 * 
 * 
 * @author Ajaykumar Prathap
 * @since 2017-03-19
 */

public class PageRankSorter extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( PageRankSorter.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new PageRankSorter(), args);
      System .exit(res);
   }
  // to run locally
   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " PageRankSorter ");
      
      Configuration conf = new Configuration();
      String[] arg = new GenericOptionsParser(conf, args).getRemainingArgs();
      
     
      job.setJarByClass( this .getClass());
      
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      
      
      job.setMapperClass( PageRankSorterMap .class);
      job.setMapOutputKeyClass(DoubleWritable.class);
      job.setMapOutputValueClass(Text.class);
      job.setSortComparatorClass(PageComparator.class);
      job.setReducerClass( PageRankSorterReducer .class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass( DoubleWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class PageRankSorterMap extends Mapper<LongWritable ,  Text , DoubleWritable, Text > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      public void map( LongWritable key,  Text value,  Context context)
        throws  IOException,  InterruptedException {
    	  String valueStr = value.toString();
    	  String[] sections = valueStr.split("\\t");
    	 
      	
      	// Get the rank and the current page (ignoring the pages at the end)
      	 float rank = Float.parseFloat(sections[1]);
      	 String page = sections[0];
      	
      	// Output rank first
      	context.write(new DoubleWritable(rank), new Text(page));
    	   
      }
   }
   
   //
   public static class PageRankSorterReducer extends Reducer<DoubleWritable ,  Text , Text, DoubleWritable > {
	     
	      private Text word  = new Text();
	   

	      public void reduce( DoubleWritable key,  Iterable<Text> values,  Context context)
	        throws  IOException,  InterruptedException {
	    	  
	    	  // it keeps title as key and final page rank as values
	    	  for (Text value : values) {
	              
	              context.write(value, key);
	              
	          }
	      	
	      
	      	
	    	   
	      }
	   }
 
  
}
