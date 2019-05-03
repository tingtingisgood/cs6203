package mylab0;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.regex.Pattern;
import java.util.Scanner;


public class SparseMartrixMultiply {
	public static class SparseMatrixMapper1 extends Mapper<LongWritable, Text, Text, Text> {
			private String flag;// m1 or m2
			private String[] itemList;
			private int itemnum=100;
			//private ArrayList<String> userList;
			//private int rowNum = 27278;// 矩阵A的行数
			//private int colNum = 27278;// 矩阵B的列数
			@Override
			protected void setup(Context context) throws IOException, InterruptedException {
				FileSplit split = (FileSplit) context.getInputSplit();
				flag = split.getPath().getName();// 判断读的数据集
				/*final Pattern DELIMITER = Pattern.compile("[,]");
				FileReader fr=new FileReader("/user/hadoop/matrix/itemid");
				BufferedReader br=new BufferedReader(fr);
				String temp=br.readLine();
				String[] tokens = DELIMITER.split(temp);
				itemList=(ArrayList)Arrays.asList(tokens);*/
				try{
					Path path= new Path("/user/hadoop/matrix/itemid");
					FileSystem fs=FileSystem.get(new Configuration());
					Scanner sc = new Scanner(fs.open(path));
					if(sc.hasNextLine()){
				        String line = sc.nextLine();
				        itemList = line.split(",");
				        for(String str:itemList){
				        	System.out.println(str);
				        }
			        }
			        sc.close();
			    }catch(Exception e){System.out.println(e);}
				
			}
			
			@Override
			public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
				String[] tokens = Mainrun.DELIMITER.split(values.toString());
				if (flag.equals("m1")) {
					String row = tokens[0];
					String col = tokens[1];
					String val = tokens[2];
					for (String id:itemList){
							Text k = new Text(row + "," + id);
							Text v = new Text("A:" + col + "," + val);
							context.write(k, v);
							System.out.println(k.toString() + " " + v.toString());
					}
				} else if (flag.equals("m2")) {
					String row = tokens[0];
					String col = tokens[1];
					String val = tokens[2];
					for (String id:itemList) {
							Text k = new Text(id + "," + col);
							Text v = new Text("B:" + row + "," + val);
							context.write(k, v);
							System.out.println(k.toString() + " " + v.toString());
					}
				}
			}
	}
	
	public static class SparseMatrixReducer1 extends Reducer<Text, Text, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, String> mapA = new HashMap<String, String>();
			Map<String, String> mapB = new HashMap<String, String>();
			for (Text line : values) {
				String val = line.toString();
				//System.out.println(key.toString() + " "+ val);
				// System.out.print("(" + val + ")");
				if (val.startsWith("A:")) {
					String[] kv = Mainrun.DELIMITER.split(val.substring(2));
					mapA.put(kv[0], kv[1]);
					// System.out.println("A:" + kv[0] + "," + kv[1]);
				} else if (val.startsWith("B:")) {
					String[] kv = Mainrun.DELIMITER.split(val.substring(2));
					mapB.put(kv[0], kv[1]);
					// System.out.println("B:" + kv[0] + "," + kv[1]);
				}
			}
			double result = 0;
			Iterator<String> iter = mapA.keySet().iterator();
			while (iter.hasNext()) {
				String mapk = iter.next();
				String bVal = mapB.containsKey(mapk) ? mapB.get(mapk) : "0";
				result += Double.parseDouble(mapA.get(mapk)) * Double.parseDouble(bVal);
			}
			context.write(key, new DoubleWritable(result));
			//System.out.println("C:" + key.toString() + "," + result);
		}
	}
	
	public static class SparseMatrixMapper2 extends Mapper<Text, Text, Text, Text> {	
		@Override
		public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
			String[] tokens=key.toString().split(",");
			Text k = new Text(tokens[0]);
			Text v = new Text(tokens[1]+","+values.toString());
			context.write(k, v);
			//System.out.println(k.toString() + " " + v.toString());
		}
}
	
	public static class SparseMatrixReducer2 extends Reducer<Text, Text, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double sum=0.0;
			Iterator<Text> iter=values.iterator();
			List<Text> cache=new ArrayList<Text>();
			while(iter.hasNext()){
                Text t=iter.next();
				cache.add(new Text(t));
				String[] tokens=t.toString().split(",");
				sum=sum+Double.parseDouble(tokens[1]);		
			}
				//Iterable 只能被遍历一次
			for (Text c : cache){
				String[] tokens=c.toString().split(",");
				Text k=new Text(key.toString()+","+tokens[0]);
				double d=Double.parseDouble(tokens[1])/sum;
				DoubleWritable v= new DoubleWritable(d);
				context.write(k,v);
			}
		}
	}
	
	public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
		JobConf conf = Mainrun.config();
		String input = path.get("input");
		String input1 = path.get("input1");
		String input2 = path.get("input2");
		String input3 = path.get("input3");
		String output1 = path.get("output1");
		String output2 = path.get("output2");
		
		Hdfs hdfs = new Hdfs(Mainrun.HDFS, conf);
		hdfs.rmr(input);
		hdfs.mkdirs(input);
		hdfs.copyFile(path.get("m1"), input1);
		hdfs.copyFile(path.get("m2"), input2);
		hdfs.copyFile(path.get("itemid"), input3);
		
		Job job1 = new Job(conf);
		job1.setJarByClass(SparseMartrixMultiply.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapperClass(SparseMatrixMapper1.class);
		job1.setReducerClass(SparseMatrixReducer1.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job1, new Path(input1), new Path(input2));// 加载2个输入数据集
		FileOutputFormat.setOutputPath(job1, new Path(output1));
		job1.waitForCompletion(true);
		
		Job job2 = new Job(conf);
		job2.setJarByClass(SparseMartrixMultiply.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(SparseMatrixMapper2.class);
		job2.setReducerClass(SparseMatrixReducer2.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2, new Path(output1));// 加载2个输入数据集
		FileOutputFormat.setOutputPath(job2, new Path(output2));
		job2.waitForCompletion(true);
	}
}