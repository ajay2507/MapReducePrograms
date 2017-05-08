
import java.io.IOException;
import java.text.DecimalFormat;
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
 * Page Parser to parse the input xml file
 * and it gets the initial page Rank and 
 * emits title as key and URL list as
 * value
 * 
 * 
 * @author Ajaykumar Prathap
 * @since 2017-03-19
 */

public class ParsePageLink extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( ParsePageLink.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new ParsePageLink(), args);
      System .exit(res);
   }
   //To run the files locally
   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " ParsePageLink ");
      
      Configuration conf = new Configuration();
      String[] arg = new GenericOptionsParser(conf, args).getRemainingArgs();
      
      conf.set("START_TAG_KEY", "<title>");
      conf.set("END_TAG_KEY", "</text>");
      job.setJarByClass( this .getClass());
      
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      
      
      job.setMapperClass( ParsePageLinkMapper .class);
      job.setReducerClass( ParsePageLinkReducer .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( Text .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   /**
    * PageParser Mapper parses title as key and links as values
    * 
    */
   
   public static class ParsePageLinkMapper extends Mapper<LongWritable ,  Text , Text, Text > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      
   
      //regex pattern to extract the link between [[ ]]
      private static final Pattern p = Pattern.compile("\\[\\[(.*?)\\]\\]");
      
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

    	  String title = "";
    	  String text = "";
    	  String links = "";
         String line  = lineText.toString();
         Text currentWord  = new Text();
       
         // Extracting values of title and links
         if(!(line.isEmpty()) && line != null && line != " "){
             title = line.split("<title>")[1].split("</title>")[0];
             String removeLine = line.replaceAll("<text.*?>", "<text>");
             text = removeLine.split("<text>")[1].split("</text>")[0];
          
         
         }
         
         String finalStr = "";
         // Matching with Regex 
         Matcher m = p.matcher(text); 
        
         while(m.find()) {
     	   	    
     	   if(title != "" && title != null){
               currentWord = new Text(title);
               context.write(currentWord,new Text(m.group(1)));
            }
       	}
         
        
         
         }
   }

   
   /**
    * PageParser Reducer parses title as key and links and URL list as values
    * 
    */
   
   public static class ParsePageLinkReducer extends Reducer<Text ,  Text ,  Text ,  Text > {
     
	   private final static DecimalFormat df  = new DecimalFormat("0.00000000");
	   @Override 
      public void reduce( Text word,  Iterable<Text> values,  Context context)
         throws IOException,  InterruptedException {
    	 // getting pageRank from the context
    	 
    	 double pageRank = context.getConfiguration().getDouble("pageRank", 0);
    	// formatting the decimal value to 8 decimal places
    	  String stored = df.format(pageRank) +"\t";
         boolean first = true;
         for (Text value : values) {
             if(!first) stored += "#####";
             stored += value.toString();
             first = false;
         }
         context.write(word,  new Text(stored));
      }
   }
}
