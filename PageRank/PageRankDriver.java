
/**
 * Driver Class to compute Page Rank, it has
 * four jobs listed below to parse and 
 * compute the page rank of each wiki document 
 * and sorts it in descending order
 * 
 * 
 * @author Ajaykumar Prathap
 * @since 2017-03-19
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.cloud.CalculateRank.CalculateRankMapper;
import org.cloud.CalculateRank.CalculateRankReducer;
import org.cloud.PageRankSorter.PageRankSorterMap;
import org.cloud.PageRankSorter.PageRankSorterReducer;
import org.cloud.ParsePageLink.ParsePageLinkMapper;
import org.cloud.ParsePageLink.ParsePageLinkReducer;
import org.cloud.TotalCount.TotalCountMapper;
import org.cloud.TotalCount.TotalCountReducer;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;


public class PageRankDriver extends Configured implements Tool{

	 
	/**
	 * @param args
	 */
	 //initialize the decimal formatter
	 private static NumberFormat init = new DecimalFormat("00");
	 public static void main(String[] args) throws Exception {
	        System.exit(ToolRunner.run(new Configuration(), new PageRankDriver(), args));
	  }
	 

	    @Override
	    public int run(String[] args) throws Exception {
	    	 
	    	 Configuration conf = new Configuration();
	    	 String outputPath = "";
	    	 // gets the input path and checks whether it has / at the end
	    	 String[] arg = new GenericOptionsParser(conf, args).getRemainingArgs();
	    	 if(args[1].substring(args[1].length() - 1) == "/"){
	    		 
	    		 args[1] = args[1].substring(0, args[1].length() - 1);
	    		 
	    	 }
	    	 
	    	 boolean isCompleted = totalCount(args[0], args[1]+"/count");
	    	 
	    	 if (!isCompleted) return 1;
	    	 
	    	 Path input = new Path(args[1]+"/count/part-r-00000");
	    	 // get total number of wiki pages
	    	 int noOfDoc = getCount(input);
	    	
	    	 double initialPageRank = 1.0 /  (double) noOfDoc;
	    	
	    	 // calls xmlParsingJob to parse the xml input
	         isCompleted = xmlParsingJob(args[0], args[1]+"/iter00",initialPageRank);
	    	 if (!isCompleted) return 1;
	         
	    	 // calls Calculate Rank for 10times iteratively for convergence
	         for (int count = 0; count < 10; count++) {
	             String inputPath = args[1]+"/iter" + init.format(count);
	             outputPath = args[1]+"/iter" + init.format(count + 1);
	             isCompleted = calculateRankJob(inputPath, outputPath);
                 if (!isCompleted) return 1;
	         }
	         
	         // calls pageRankSorter job to sort the input job and setting output as destination folder
	         isCompleted = pageRankSorterJob(outputPath, args[1]+"/output");
	         
	         //deleting all the intermediatory folders 
	         for (int count = 0; count <= 10; count++) {
	        	 deleteFolder(conf, args[1]+"/iter" + init.format(count ));
	         }
	         deleteFolder(conf,args[1]+"/count");
	         if (!isCompleted) return 1;
	         return 0;
	    	 
	    	

	    }
	    
	    /**
	     * To count total number of documents in the wiki page by 
	     * counting all the <title> tags
	     * 
	     */
	    public boolean totalCount(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
	        Configuration conf = new Configuration();
	     

	        Job getCount = Job.getInstance(conf, "totalCount");
	        getCount.setJarByClass(PageRankDriver.class);
 
	        //setting input and output path
	        FileInputFormat.addInputPaths(getCount,  inputPath);
	        FileOutputFormat.setOutputPath(getCount,  new Path(outputPath));
	        
	         // Input -> Mapper -> Map
	        getCount.setMapperClass( TotalCountMapper .class);
	        // Reduce -> Reducer -> Output
	        getCount.setReducerClass( TotalCountReducer .class);
	        getCount.setOutputKeyClass( Text .class);
	        getCount.setOutputValueClass( IntWritable .class);

	        return getCount.waitForCompletion( true);
	    }
	    
	    
	    
	    public boolean xmlParsingJob(String inputPath, String outputPath, double initialPageRank) throws IOException, ClassNotFoundException, InterruptedException {
	        Configuration conf = new Configuration();
	        conf.set(XmlInputFormat.START_TAG_KEY, "<title>");
	        conf.set(XmlInputFormat.END_TAG_KEY, "</text>");
	        conf.setDouble("pageRank", initialPageRank);
	        Job xmlParsing = Job.getInstance(conf, "xmlParsing");
	        xmlParsing.setJarByClass(PageRankDriver.class);

	        //setting input and output path
	        FileInputFormat.addInputPaths(xmlParsing,  inputPath);
	        FileOutputFormat.setOutputPath(xmlParsing,  new Path(outputPath));
	        
	        // Input -> Mapper -> Map
	        xmlParsing.setMapperClass( ParsePageLinkMapper .class);
	        // Reduce -> Reducer -> Output
	        xmlParsing.setReducerClass( ParsePageLinkReducer .class);
	        xmlParsing.setOutputKeyClass( Text .class);
	        xmlParsing.setOutputValueClass( Text .class);

	        return xmlParsing.waitForCompletion( true);
	    }
	    
	    
	  //Calculation MapReduce Job 2
	    private boolean calculateRankJob(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
	        Configuration conf = new Configuration();

	        Job calculatePageRank = Job.getInstance(conf, "runCalculatePageRank");
	        calculatePageRank.setJarByClass(PageRankDriver.class);

	        // Input -> Mapper -> Map
	        calculatePageRank.setOutputKeyClass(Text.class);
	        calculatePageRank.setOutputValueClass(Text.class);
	        calculatePageRank.setMapperClass(CalculateRankMapper.class);

	        // Reduce -> Reducer -> Output
	        FileInputFormat.setInputPaths(calculatePageRank, new Path(inputPath));
	        FileOutputFormat.setOutputPath(calculatePageRank, new Path(outputPath));
	        calculatePageRank.setReducerClass(CalculateRankReducer.class);

	        return calculatePageRank.waitForCompletion(true);
	    }
	    
	    //Sorting Map Job 3
	    private boolean pageRankSorterJob(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
	        Configuration conf = new Configuration();

	        Job pageRankSorter = Job.getInstance(conf, "pageRankSorterJob");
	        pageRankSorter.setJarByClass(PageRankDriver.class);

	        // Input -> Mapper -> Map
	       
	        pageRankSorter.setMapOutputKeyClass(DoubleWritable.class);
	        pageRankSorter.setMapOutputValueClass(Text.class);
	        pageRankSorter.setMapperClass(PageRankSorterMap.class);
	        
	        // comparator to sort the rank in descending order
	        pageRankSorter.setSortComparatorClass(PageComparator.class);
	        
	        // Reduce -> Reducer -> Output
	        pageRankSorter.setReducerClass(PageRankSorterReducer.class);
	        pageRankSorter.setOutputKeyClass(Text.class);
	        pageRankSorter.setOutputValueClass( DoubleWritable .class);
	        FileInputFormat.setInputPaths(pageRankSorter, new Path(inputPath));
	        FileOutputFormat.setOutputPath(pageRankSorter, new Path(outputPath));
	        pageRankSorter.setInputFormatClass(TextInputFormat.class);
	        pageRankSorter.setOutputFormatClass(TextOutputFormat.class);

	        return pageRankSorter.waitForCompletion(true);
	    }
	    
	    //checks and deletes the folder if the folder is present
	    private static void deleteFolder(Configuration conf, String folderPath ) throws IOException {
			// Delete the Folder
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(folderPath);
			if(fs.exists(path)) {
				fs.delete(path,true);
			}
		}
	    
	    // to get the total count 
	    public static int getCount(Path file) throws IOException {
	    	System.out.println("inside numnodes method");
		    Configuration conf = new Configuration();
		    
		    FileSystem fs = file.getFileSystem(conf);
		    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(file)));
		    String line;
		    String count="";
            line=br.readLine();
            if (line != null && !line.isEmpty()){
                System.out.println(line);
                //line=br.readLine();
                count = line.split("=")[1];
            }
		    return Integer.parseInt(count.trim());
		  }
	    
	    

}
