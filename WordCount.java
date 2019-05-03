package mylab0;


import java.io.IOException;
import java.util.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
	   
	public static class Map extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
	     private Text word = new Text();
	     Set<String> stopwords = new HashSet<String>();
	     @Override
	     protected void setup(Context context){
	        Configuration conf = context.getConfiguration();
	        try {
	         Path path = new Path("/user/hadoop/commonwords/stopwords/sw4.txt");
	         FileSystem fs= FileSystem.get(new Configuration());
	         BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
	         String word = null;
	         while ((word= br.readLine())!= null) {
	         stopwords.add(word);
	         }
	     } catch (IOException e) {
	      e.printStackTrace();
	     }
	    }
	     
	     public void map(Object key, Text value, Context context ) throws IOException,InterruptedException{
	       String line = value.toString();
	       //StringTokenizer tokenizer = new StringTokenizer(line," \t\n\r\f,.:;?![]'");
           StringTokenizer tokenizer = new StringTokenizer(line," . ! ? ; [ ] , ' \r \n \r \f");
	       while (tokenizer.hasMoreTokens()) {
	         word.set(tokenizer.nextToken().toLowerCase());
	         if(stopwords.contains(word.toString()))
	        	 continue;
	         context.write(word, one);
	       }
	     }	   
	   }

	   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		   public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {	       
	       int sum = 0;
	       for(IntWritable val : values){
	    	   sum += val.get();
	       }
	       context.write(key, new IntWritable(sum));
	     }
	   }
	
	   public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 Job job0 = new Job(conf, "wordcount");
	     job0.setJarByClass(WordCount.class);
			//set input and output for MapReduce job 2 here
			//set Mapper and Reduce class, output key, value class 
		 job0.setOutputKeyClass(Text.class);
		 job0.setOutputValueClass(IntWritable.class);
		 job0.setMapperClass(Map.class);
		 job0.setCombinerClass(Reduce.class);
	     job0.setReducerClass(Reduce.class);
	     job0.setInputFormatClass(TextInputFormat.class);
	     //job0.setOutputFormatClass(TextOutputFormat<K,V>.class );
	     FileInputFormat.setInputPaths(job0, new Path(args[0]));
	     FileOutputFormat.setOutputPath(job0, new Path(args[1]));
	     System.exit(job0.waitForCompletion(true) ? 0 : 1);
		   /*JobConf conf = new JobConf(WordCount.class);
		   conf.setJobName("wordcount");
		   	
		   conf.setOutputKeyClass(Text.class);
		   conf.setOutputValueClass(IntWritable.class);
		   conf.setMapperClass(Map.class);
		   conf.setCombinerClass(Reduce.class);
		   conf.setReducerClass(Reduce.class);
		   
		   conf.setInputFormat(TextInputFormat.class);
		   conf.setOutputFormat(TextOutputFormat.class);
		   
		   FileInputFormat.setInputPaths(conf, new Path(args[0]));
		   FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		   
		   JobClient.runJob(conf);*/
	   }
	}

