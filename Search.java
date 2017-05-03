/**
 * 
 */
package org.myorg;

/**
 * Cloud Assignment
 * Implement Search Functionality
 * 
 * AUTHOR Ajaykumar Prathap
 * EMAIL  aprathap@uncc.edu
 *
 */
import java.io.IOException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;


public class Search extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger(Search.class);

   public static void main( String[] args) throws  Exception {
	   int res  = ToolRunner .run( new Search(), args);
	   
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	
	  
	   Configuration conf = new Configuration();
	   conf.set("query",args[2]);
	  
      Job job  = Job .getInstance(conf, " Search ");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      
      FileOutputFormat.setOutputPath(job,  new Path(args[1]));
      
      
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputValueClass( DoubleWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      private final static DoubleWritable one  = new DoubleWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	 // int length = (int)context.getConfiguration().getLong("mapreduce.input.num.files",1);
         String line  = lineText.toString().toLowerCase();
         
         Text currentWord  = new Text();
         String finalStr = "";
        
         String[] parts = line.split("#####");
         String[] parts1 = parts[1].split("\t");
         String search = parts[0].replaceAll("\\s+","");
        
        //Getting User Query from Config Object
         Configuration config= context.getConfiguration();
         String query = config.get("query");
         String[] querylist = query.split(" ");
        
         if((querylist[0].toLowerCase().equalsIgnoreCase(search.toLowerCase())) || 
        		 querylist[1].toLowerCase().equalsIgnoreCase(search.toLowerCase())){
            context.write(new Text(parts1[0]), new Text(parts1[1]));
            
         }
         
            currentWord  = new Text(word);
          
         
      }
   }

   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<Text > counts,  Context context)
         throws IOException,  InterruptedException {
         
         double sum = 0.0;
         Configuration config= context.getConfiguration();
       
          HashMap<String, String> map = new HashMap<String,String>();
       
         for ( Text count  : counts) {
        	 sum = sum + Double.parseDouble(count.toString());
        	 }
         context.write(word, new DoubleWritable(sum));
        
                
      }
   }
}


