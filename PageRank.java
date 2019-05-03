package mylab0;

import java.io.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//import com.sun.xml.internal.bind.v2.schemagen.xmlschema.List;

public class PageRank {
	public static int count=1458;
	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			// parse an input line into page, pagerank, outgoing links
			StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
			String page=null;
			double pagerank=0;
			List<String> outgoingLinks=new ArrayList<String>();
			for(int i=0;itr.hasMoreTokens()==true;i++) {
				if(i==0) page=itr.nextToken();
				else if(i==1) pagerank=Double.parseDouble(itr.nextToken());
				else outgoingLinks.add(itr.nextToken());
			}
			StringBuilder sb=new StringBuilder();
			if(!outgoingLinks.isEmpty()){
				for(String a:outgoingLinks){
					context.write(new Text(a), new Text(""+pagerank/outgoingLinks.size()));
					sb.append(a+"\t");
				}
			}
			context.write(new Text(page),new Text("EDGE:"+sb.toString()));	    		   
	    }
	}

	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			// analyze values, if the value starts with EDGE:, then the phrase
			// after EDGE: are outgoing links
			// sum up the values that do not start with EDGE: into a variable S
			// compute new pagerank as 0.15/N+0.85*S (N is the total number of nodes)
			double credit = 0.0;
			String outgoingLinks=null;
			for(Text val : values){
				if (val.toString().startsWith("EDGE")){
				    outgoingLinks=val.toString().replaceFirst("EDGE:","");
				}
				else{
				    credit += Double.parseDouble(val.toString());
				}
			}
			//Configuration conf=context.getConfiguration();
			//int numNode = Integer.parseInt(conf.get("numNode"));
			double pageRank = 0.85*credit + 0.15/(1.0*PageRank.count);
			// output (key, newpagerank + outgoing links)
			context.write(key,new Text(""+pageRank+"\t"+outgoingLinks));	
		}

	}
	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
		}

	public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("numNode", ""+PageRank.count);
		if (args.length!= 3) {
			System.err.println("Usage: PageRank <in> <out> <iterations>");
			System.exit(1);
		}
		Path inPath=new Path(args[0]);
        Path outPath=null;
        int iterations = new Integer(args[2]);
		for (int i = 0; i < iterations; i++) {
			// create a new job, set job configurations and run the job
			outPath = new Path(args[1]+i);
            Job job = new Job(conf,"PageRank");
            job.setJarByClass(PageRank.class);
            job.setMapperClass(Mapper1.class);
            job.setReducerClass(Reducer1.class);
            job.setMapOutputKeyClass(Text.class);
    		job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths(job, inPath);
            FileOutputFormat.setOutputPath(job, outPath);
            job.waitForCompletion(true);
            inPath = outPath;
		}
		/*Job job = new Job(conf,"sorting");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(Mapper2.class);
        job.setReducerClass(Reducer2.class);
        job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        job.waitForCompletion(true);
        */
		
	}
	}
}


/*
 * Count the number of nodes and attach a pagerank score 
 * use a bufferedReader to read every line of file, modify the line, save it in a List. 
 * use a bufferedWriter to rewrite the every line in the List to file again.
 */

/*
  public static void preprocessing(String filename) {

	try{
		Path pathOutput = new Path("/user/hadoop/PageRank/input/preprocess.txt");
		Path pathinput = new Path(filename);
		FileSystem fs=FileSystem.get(new Configuration());
		Scanner sc = new Scanner(fs.open(pathinput));
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(fs.create(pathOutput)));
		while (sc.hasNextLine()) {
	        String line = sc.nextLine();
	        String[]  graph = line.split("\t");
	        // add score value inside graph
	        String output = "";
	        for(int i=0; i<graph.length; i++){
	        	if(i==0){
	        		output+=(graph[i]+"\t1.0");
	        	}
	        	else{
	        		output+=("\t"+graph[i]);
	        	}
	        }
	        pw.println(output);
            PageRank.count++;
        }
        sc.close();
        pw.close();
    }catch(Exception e){System.out.println(e);}
}
*/


