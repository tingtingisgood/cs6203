package mylab0;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.lang.Integer;
import java.lang.Math;

import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;//get the file path and name
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SearchDocuments {

	// Stage 1: Compute frequency of every word in a document
	// Mapper 1: (tokenize file)
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		Set<String> stopwords = new HashSet<String>();

		@Override
		protected void setup(Context context) {
			//read in stopwords file
			//Read stopwords from HDFS and put it into the Set<String> stopwords
			Configuration conf = context.getConfiguration();
			try {
			Path path = new Path("/user/hadoop/SearchDocuments/stopwords/sw4.txt");
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

		private Text word_filename = new Text();
		private final static IntWritable one = new IntWritable(1);
		private String pattern = "[^\\w]";
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Get file name for a key/value pair in the Map function
            String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
			// read one line. tokenize into (word@filename, 1) pairs
			String line = value.toString().toLowerCase();  //convert to lowercase letters
			line = line.replaceAll(pattern," ");//replace the elements in pattern with spaces
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				word_filename.set(itr.nextToken());
				//check whether the word is in stopword list
				if(stopwords.contains(word_filename.toString()))
					continue;
				word_filename = new Text(word_filename.toString().concat('@' + fileName));
				context.write(word_filename,one);
			}
		}
	}

	// Reducer 1: (calculate frequency of every word in every file)
	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// sum up all the values, output (word@filename, freq) pair
		       int sum = 0;
			   for(IntWritable val : values){
		    	   sum += val.get();
		       }
			   result.set(sum);
		       context.write(key,result);
		}
	}

	// Stage 2: Compute tf-idf of every word w.r.t. a document
	// Mapper 2:parse the output of stage1
	public static class Mapper2 extends Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			// parse the key/value pair into word, filename, frequency
             String[] a = key.toString().split("@");
			// output a pair (word, filename=frequency)
             context.write(new Text(a[0]),new Text(a[1]+'='+ value.toString()));
		}
	}

	// Reducer 2: (calculate tf-idf of every word in every document)
	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Note: key is a word, values are in the form of
			// (filename=frequency)
			// sum up the number of files containing a particular word
			int idf = 0;
			double tf_idf = 0.0;
			int tf = 0;
			List<String> valueList = new ArrayList<String>();
			for(Text val : values){
				idf++;
				valueList.add(val.toString());
			}
			// for every filename=frequency in the value, compute tf-idf of this
			// word in filename and output (word@filename, tfidf)
			for(String temp : valueList){
			    String[] field = temp.split("=");
				tf = Integer.parseInt(field[1]);
				tf_idf = (1 + Math.log(tf)) * Math.log(10.0/idf);//the number of document is ten for this project
				context.write(new Text(key.toString()+'@'+field[0]),new Text(String.valueOf(tf_idf)));
			}
		}
	}

	// Stage 3: Compute normalized tf-idf
	// Mapper 3: (parse the output of stage 2)
	public static class Mapper3 extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			// parse the key/value pair into word, filename, tfidf
            String[] a = key.toString().split("@");
            // output a pair(filename, word=tfidf)
            context.write(new Text(a[1]),new Text(a[0]+'='+value.toString()));
		}
	}

	// Reducer 3: (compute normalized tf-idf of every word in very document)
	public static class Reducer3 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Note: key is a filename, values are in the form of (word=tfidf)
			// for every word=tfidf in the value, output (word@filename,norm-tfidf)
             double sum_square = 0;
             List<String> valueList = new ArrayList<String>();	
             for(Text val : values){
            	String[] a1 = val.toString().split("=");
            	double tf_idf = Double.parseDouble(a1[1]);
            	sum_square += Math.pow(tf_idf, 2);
            	valueList.add(val.toString());
             }
             for(String temp :valueList){
            	String[] a2 = temp.split("=");
            	double tf_idf = Double.parseDouble(a2[1]);
            	double norm_tfidf = tf_idf/Math.sqrt(sum_square);
            	context.write(new Text(a2[0]+'@'+key.toString()),new Text(String.valueOf(norm_tfidf)));
             }	
		}
	}

	// Stage 4: Compute the relevance of every document w.r.t. a query
	// Mapper 4: (parse the output of stages)
	public static class Mapper4 extends Mapper<Text, Text, Text, Text> {

		Set<String> query = new HashSet<String>();

		@Override
		protected void setup(Context context) {
			//set up query 
			Configuration conf = context.getConfiguration();
			try {
			Path path = new Path("/user/hadoop/SearchDocuments/query/query.txt");
			FileSystem fs= FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String word = null;
			while ((word= br.readLine())!= null) {
			query.add(word);
			}
			} catch (IOException e) {
			e.printStackTrace();
			}
		}

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			// parse the key/value pair into word, filename, norm-tfidf
            String[] a = key.toString().split("@");
			// if the word is contained in the query file, output (filename,word=norm-tfidf)
            if(query.contains(a[0])) context.write(new Text(a[1]), new Text(a[0]+'='+value.toString()));
		}
	}

	// Reducer 4: (calculate relevance of every document w.r.t. the query)
	public static class Reducer4 extends Reducer<Text, Text, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Note: key is a filename, values are in the form of(word=norm-tfidf)
			double sum = 0.0;
            for(Text val : values){
            	String[] a = val.toString().split("=");
            	double norm_tfidf = Double.parseDouble(a[1]);
            	sum += norm_tfidf;
            }
            context.write(key, new DoubleWritable(sum));
		}
	}

	// Stage 5: Do the sorting here
	// Get local Top K=3
	//output in decreasing order on key
	public static class Mapper5 extends Mapper<Text, Text, IntWritable, Text> {
		private final static IntWritable one = new IntWritable(1);
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
            // output a pair(one,filename=relevance)
            context.write(one,new Text(key.toString()+'='+value.toString()));
		}
	}
	
	public static class Reducer5 extends Reducer<IntWritable, Text, Text, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Note: key is a filename, values are in the form of(word=norm-tfidf)
			String top1Filename=""; double relevance1=0.0;
			String top2Filename=""; double relevance2=0.0;
			String top3Filename=""; double relevance3=0.0;
            for(Text val : values){
            	String[] field = val.toString().split("=");
            	String filename=field[0];
            	double relevance = Double.parseDouble(field[1]);
            	if(relevance > relevance1){    		
            		top3Filename = top2Filename;
            		relevance3 = relevance2;
            		top2Filename = top1Filename;
            		relevance2 = relevance1;
            		top1Filename = filename;
            		relevance1=relevance;
            	}
            	else{
            		if(relevance > relevance2){
            			top3Filename = top2Filename;
                		relevance3 = relevance2;
            			top2Filename = filename;
            			relevance2 = relevance;
            		}
            		else{
            		    if(relevance > relevance3){
            			    top3Filename = filename;
                		    relevance3 = relevance;
            		    }
            		}	
            	}
            }
            context.write(new Text(""),new Text(top1Filename));
            context.write(new Text(""),new Text(top2Filename));
            context.write(new Text(""),new Text(top3Filename));
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 6) {
			System.err
					.println("Usage: searchdocuments <documents directory> <output1> "
							+ "<output2> <output3> <output4> <output5>");
			System.exit(2);
		}

		// Stage 1: Compute frequency of every word in a document
		Job job1 = new Job(conf, "word-filename count");
		job1.setJarByClass(SearchDocuments.class);
		FileInputFormat.setInputPaths(job1,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1,new Path(otherArgs[1]));
		job1.setMapperClass(TokenizerMapper.class);
		job1.setReducerClass(IntSumReducer.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.waitForCompletion(true);
		// Stage 2: Compute tf-idf of every word w.r.t. a document
		Job job2 = new Job(conf, "calculate tfidf");
        job2.setJarByClass(SearchDocuments.class);
        FileInputFormat.setInputPaths(job2,new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job2,new Path(otherArgs[2]));
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		job2.waitForCompletion(true);
		// Stage 3: Compute normalized tf-idf
		Job job3 = new Job(conf, "nomalize tfidf");
        job3.setJarByClass(SearchDocuments.class);
        FileInputFormat.setInputPaths(job3,new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job3,new Path(otherArgs[3]));
		job3.setMapperClass(Mapper3.class);
		job3.setReducerClass(Reducer3.class);
		job3.setInputFormatClass(KeyValueTextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DoubleWritable.class);
		job3.waitForCompletion(true);
		// Stage 4: Compute the relevance of every document w.r.t. a query
		Job job4 = new Job(conf, "compute revelance");
        job4.setJarByClass(SearchDocuments.class);
        FileInputFormat.setInputPaths(job4,new Path(otherArgs[3]));
		FileOutputFormat.setOutputPath(job4,new Path(otherArgs[4]));
		job4.setMapperClass(Mapper4.class);
		job4.setReducerClass(Reducer4.class);
		job4.setInputFormatClass(KeyValueTextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(DoubleWritable.class);
		job4.waitForCompletion(true);
		// Stage 5: Get topk documents
		Job job5 = new Job(conf, "get top K documents");
        job5.setJarByClass(SearchDocuments.class);
        FileInputFormat.setInputPaths(job5,new Path(otherArgs[4]));
		FileOutputFormat.setOutputPath(job5,new Path(otherArgs[5]));
		job5.setMapperClass(Mapper5.class);
		job5.setReducerClass(Reducer5.class);
		job5.setInputFormatClass(KeyValueTextInputFormat.class);
		job5.setOutputFormatClass(TextOutputFormat.class);
		job5.setMapOutputKeyClass(IntWritable.class);
		job5.setMapOutputValueClass(Text.class);
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(Text.class);
		job5.waitForCompletion(true);
		System.exit(job5.waitForCompletion(true) ? 0 : 1);
	}

}
