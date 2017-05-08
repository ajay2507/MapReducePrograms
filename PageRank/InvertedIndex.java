

/**
 * To Find Inverted index, Mapper takes the words 
 * from the text tag and the respective title, Reducer
 * combines all the titles respective to each word and 
 * counts the number of times the word appears in the 
 * particular document.
 * 
 * 
 * @author Ajaykumar Prathap
 * @version 1.0
 * @since 2017-03-22
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.event.DocumentListener;

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


public class InvertedIndex extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( InvertedIndex.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new InvertedIndex(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " InvertedIndex ");
      
      Configuration conf = new Configuration();
      String[] arg = new GenericOptionsParser(conf, args).getRemainingArgs();
      
      
      job.setJarByClass( this .getClass());
      //set input and output file path
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]+"/invertedindex"));
      
      //setting inverted index mapper and reducer
      job.setMapperClass( InvertedIndexMapper .class);
      job.setReducerClass( InvertedIndexReducer .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( Text .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   /**
    * Inverted Index Mapper splits each word and emits word
    * and respective title for all documents
    * 
    */
   public static class InvertedIndexMapper extends Mapper<LongWritable ,  Text , Text, Text > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      //Regex to select only words and eliminate all special characters
      private static final Pattern p = Pattern.compile("[^\\w]");
      
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

    	  String title = "";
    	  String text = "";
    	  String links = "";
         String line  = lineText.toString();
         Text currentWord  = new Text();
         
        // Extracting values of title and all words inside text document
         if(!(line.isEmpty()) && line != null && line != " "){
             title = line.split("<title>")[1].split("</title>")[0];
             String removeLine = line.replaceAll("<text.*?>", "<text>");
             text = removeLine.split("<text>")[1].split("</text>")[0];
          
         
         }
         String split[] = text.split("[^\\w]");
         
         String finalStr = "";
         // Matching with Regex 
         Matcher m = p.matcher(text);
         //iterates all the words after spliting 
         for(int i =0; i<split.length;i++){
        
         split[i] = split[i].replaceAll("\\d+(?:[.,]\\d+)*\\s*", "");
         
         if(title != "" && title != null && !split[i].isEmpty() && split[i].trim() != "" && split[i] != null){
             currentWord = new Text(title);
             //emiting word as key and title as value
             context.write(new Text(split[i]),currentWord);
          }}
        
         
         
   }
   }
   
   /**
    * Inverted Reducer takes each word combines the titles for
    * all the documents containing the same word. It emits the 
    * word as key and title,count as its value. Count be number 
    * of times the word appeared in the document. 
    * 
    */
   public static class InvertedIndexReducer extends Reducer<Text ,  Text ,  Text ,  Text > {
      @Override 
      public void reduce( Text key,  Iterable<Text> values,  Context context)
         throws IOException,  InterruptedException {
    	  //initialize the hash map
    	  HashMap<String,Integer> mapValue=new HashMap<String,Integer>();
    	  //initialize the counter
    	  int count = 0;
    	  //iterating all the titles for the given word
    	  for (Text value : values) {
        	 
        	 String str = value.toString();
        	 if(mapValue!=null &&mapValue.get(str)!=null){
        		 //count the total number of documents the word appeared
        		 count=(int)mapValue.get(str);
        		 mapValue.put(str, ++count);
        		  
        		 }else{
        		  
                  mapValue.put(str, 1);
        		  
        		 }
             
         }
         // Emit the output key value pair
         context.write(new Text(key+" "+new Text("===>")),  new Text(mapValue.toString()));
         
      }
   }
}


