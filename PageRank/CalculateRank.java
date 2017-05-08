import java.io.IOException;
import java.util.Iterator;
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


/**
 * Calculate Rank takes title as a Key
 * and calculates new page rank for all
 * the outlinks 
 * 
 * 
 * 
 * @author Ajaykumar Prathap
 * @since 2017-03-19
 */

public class CalculateRank extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( CalculateRank.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new CalculateRank(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " CalculateRank ");
      
      Configuration conf = new Configuration();
      String[] arg = new GenericOptionsParser(conf, args).getRemainingArgs();
      
     
      job.setJarByClass( this .getClass());
      
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      
      
      job.setMapperClass( CalculateRankMapper .class);
      job.setReducerClass( CalculateRankReducer .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( Text .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   /**
   *
   * Mapper emits pagerank and outlinks / title and URL list
   */
   
   public static class CalculateRankMapper extends Mapper<LongWritable ,  Text , Text, Text > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      public void map( LongWritable offset,  Text value,  Context context)
        throws  IOException,  InterruptedException {

    	  String title = "";
    	  String text = "";
    	  String links = "";
         
         Text currentWord  = new Text();
        
         String line = value.toString();
        
     	String[] pageLinks = line.split("\\t");
     	 //System.out.println("lineee"+pageLinks[0]);
     	 //get the page which is mapped
     	 String mapPage = pageLinks[0];
         
         // Gets the [thisPage]	[thisPagesRank]
         String mapPageStr = pageLinks[0] + "\t" + pageLinks[1] + "\t";
         
         // Ignore if page contains no links
         if(pageLinks.length < 3 || pageLinks[2] == "")
         {
         	context.write(new Text(mapPage), new Text("! "));
         	return;
         }
         
         //Get all the page links
         String linkedPages = pageLinks[2];
         String[] pages = linkedPages.split("#####");
         //Get total number of links
         int total = pages.length;
         
         //For each page we are emitting total pages and its count
         for (String page : pages) {
        	
        	 context.write(new Text(page), new Text(mapPageStr + total));
         }
         
         //original page and its original links
         if(!mapPage.isEmpty() && mapPage.trim() != ""){
        // System.out.println("map pageeeeeee"+mapPage);
         context.write(new Text(mapPage), new Text("! " + linkedPages));
         }
        
         }
   }

   /**
   *
   * Reducer calculates new page rank iteratively and emits title as key
   * and new page & URL list as value
   */
   
   public static class CalculateRankReducer extends Reducer<Text ,  Text ,  Text ,  Text > {
      @Override 
      public void reduce( Text page,  Iterable<Text> values,  Context context)
         throws IOException,  InterruptedException {
    	//  System.out.println("Calculate Rank Reducerrrrrrrrrr");
    	  String linkstr = "";
    	//given Damping factor
    	  double d = 0.85; 
    	//Initialize new page rank
    	 double newPageRank = 0.0;
    	// Initialize page rank of other links
    	  float otherPageRank = 0;
    	// iterating the values
    	  Iterator<Text> value = values.iterator();
        // Extracting all the values
    	  int inlinkcount=0;
    	  while(value.hasNext()){
    		  
    		  String pageDetails = value.next().toString();
    		  //System.out.println("Page Detailssssss"+pageDetails);
    		  
    		  if (pageDetails.startsWith("! ")) {
                  linkstr = pageDetails.substring(2);
                //  System.out.println("link str "+linkstr);
    		  }
    		  else{
    			  String[] pageLinks = pageDetails.split("\\t");
    	          float currentPageRank = Float.valueOf(pageLinks[1]);
    	          int linkCount = Integer.valueOf(pageLinks[2]);
    	          //System.out.println("outlink"+linkCount+"current page"+currentPageRank);
    	          otherPageRank += currentPageRank/linkCount;
    	          inlinkcount++;
    		  }

    		  
    	  }
    	   if(linkstr!="" && linkstr!=null){
    	    //substitute value in page rank formulae
    	   newPageRank = (1-d) + d * otherPageRank;
    	   //Add new PageRank to total
    	   context.write(page, new Text(newPageRank + "\t" + linkstr));}
      }
   }
}
