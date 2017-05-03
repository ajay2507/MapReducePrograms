/**
 * 
 */
package org.myorg;

/**
 * Cloud Assignment
 * Find TFIDF
 * 
 * AUTHOR Ajaykumar Prathap
 * EMAIL  aprathap@uncc.edu
 *
 */
//import org.myorg.*;

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
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;


public class TFIDF extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger(TFIDF.class);

   public static void main( String[] args) throws  Exception {
	   //JobConf job = (JobConf) getConf();
	   int fsjob  = ToolRunner .run( new TermFrequency(), args);
	   int res  = ToolRunner .run( new TFIDF(), args);
	   
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	   FileSystem fs = FileSystem.get(getConf());
	   Path path = new Path(args[0]);
	   ContentSummary cs = fs.getContentSummary(path);
	   long fileCount = cs.getFileCount();
	  
	   Configuration conf = new Configuration();
	   conf.set("length",String.valueOf(fileCount));
      Job job  = Job .getInstance(conf, " TFIDF ");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[1]);
      
      FileOutputFormat.setOutputPath(job,  new Path(args[ 2]));
      
      
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
         String line  = lineText.toString();
         
         Text currentWord  = new Text();
         String finalStr = "";
         // Extracting the file Name from MapReduce File split 
         String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
         
         String[] parts = line.split("#####");
         String[] parts1 = parts[1].split("\t");
         
         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
          
            currentWord  = new Text(word);
          
            //Made Changes to print file name
            if("" != fileName && !(fileName.equalsIgnoreCase(null))){
           	 
           	 
           	 finalStr = currentWord.toString()+"#####" + fileName;
           	 currentWord = new Text(finalStr);
            }
            
            context.write(new Text(parts[0]), new Text(parts1[0]+"="+parts1[1]));
           
         }
         
      }
   }

   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<Text > counts,  Context context)
         throws IOException,  InterruptedException {
         double sum  = 0.0;   
         int counter = 0;
         double IDF = 0.0;
         int length = 0;
         double TFIDF = 0.0;
         Text finalWord = new Text();
         Configuration config= context.getConfiguration();
        
         String totalFiles = config.get("length");
         length = Integer.valueOf(totalFiles);
        
        
        // Configuration config= context.getConfiguration();
        
        
          HashMap<String, String> map = new HashMap<String,String>();
       
         // putting all words as value and corresponding Term frequency as key in hash map
         for ( Text count  : counts) {
        	
        	 
        	 if(map.containsKey(count.toString()) && (map.containsValue(word.toString()))){
            	
             }else{
            
            	 counter++;
            	 map.put(count.toString(), word.toString());
            	 }
            }
        
        // Iterating the hash map to find TFIDF
         for(String values: map.keySet()){
        	 String[] parts = values.split("=");
             sum = Double.valueOf(parts[1]);
              IDF = Math.log10(1+(length/counter));
              TFIDF = sum * IDF;
              finalWord = new Text(word.toString() + "#####" + parts[0]);
             context.write(finalWord, new DoubleWritable(TFIDF));
         }
         
      
      }
   }
}

