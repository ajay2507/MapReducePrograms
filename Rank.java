/**
 * 
 */
package org.myorg;

/**
 * Cloud Assignment
 * Rank the search hits in descending order
 * 
 * AUTHOR Ajaykumar Prathap
 * EMAIL  aprathap@uncc.edu
 *
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;

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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class Rank extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger(TFIDF.class);

   public static void main( String[] args) throws  Exception {
	   int res  = ToolRunner .run( new Rank(), args);
	   System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	
	  
      Job job  = Job .getInstance(getConf(), " Rank ");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      
      FileOutputFormat.setOutputPath(job,  new Path(args[1]));
      
  //    job.setSortComparatorClass(RankComparator.class);
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setMapOutputValueClass(MapWritable.class);
      job.setOutputValueClass( DoubleWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  MapWritable > {
      private final static DoubleWritable one  = new DoubleWritable( 1);
      private Text word  = new Text();
      MapWritable map = new MapWritable();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	 // int length = (int)context.getConfiguration().getLong("mapreduce.input.num.files",1);
         String line  = lineText.toString();
         //HashMap<String, String> map = new HashMap<String,String>();
         Text currentWord  = new Text();
         String finalStr = "";
         
         System.out.println("input"+line);
       
         String[] parts = line.split("\t");
         map.put(new Text(parts[0]), new Text(parts[1]));
         context.write(currentWord, map);
         
            
          
         
      }
   }

   public static class Reduce extends Reducer<Text ,  MapWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<MapWritable> values,  Context context)
         throws IOException,  InterruptedException {
         
         double sum = 0.0;
         HashMap<String, Double> map = new HashMap<String,Double>();
         
         //Sorting the values of hash map in descending order
         for (MapWritable entry : values) {
        	  for (Entry<Writable, Writable> extractData: entry.entrySet()) {
        	      map.put(extractData.getKey().toString(),Double.valueOf(extractData.getValue().toString()));
        	   }                    
        	}
        // System.out.println("count"+count);
         
        /// map.put(String.valueOf(word), Double.parseDouble(count.toString()));
         List<java.util.Map.Entry<String, Double>> list =
                 new LinkedList<java.util.Map.Entry<String, Double>>(map.entrySet());
         
         Collections.sort(list, new Comparator<java.util.Map.Entry<String, Double>>() {
             public int compare(java.util.Map.Entry<String, Double> o1,
            		 java.util.Map.Entry<String, Double> o2) {
                 return (o2.getValue()).compareTo(o1.getValue());
             }
         });
         
         java.util.Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
         for (java.util.Map.Entry<String, Double> entry : list) {
             sortedMap.put(entry.getKey(), entry.getValue());
         }

          
        	    for (Entry<String, Double> entry : sortedMap.entrySet())
                {
                    System.out.println("Key : " + entry.getKey() + " Value : "+ entry.getValue());
                    context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue())); 
                }
         
         
         
         
         
         
         
         
                
      }
   }
}


