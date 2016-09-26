package mylab0;

import java.io.*;
import java.lang.ArrayIndexOutOfBoundsException;

public class Preprocess {
	public static void main(String[] args) throws IOException{
		File file=new File(args[0]);
		if(!file.exists()){
			file.createNewFile();
		}
		else System.err.println("file already existed!");		
		//scheme1:String[] strList=line.split(string regex[,int limit]);convert a string object to string array
		try{
			FileReader fr=new FileReader(args[1]);
			BufferedReader br=new BufferedReader(fr);
			FileWriter fw=new FileWriter(args[0]);
			PrintWriter pw=new PrintWriter(fw);
			String temp="";
			int count=0;
			while((temp=br.readLine())!=null){
				String[] str=temp.split("\t",2);
				if(str.length==1){
					temp=str[0]+"\t1.0";
				}
				else temp=str[0]+"\t1.0\t"+str[1];	
				pw.write((temp+"\n").toCharArray());
				count++;
			}
			System.out.print("the total number of lines is:"+count);
			br.close();
			pw.close();
		}catch(FileNotFoundException ee){
			ee.printStackTrace();
		}catch(ArrayIndexOutOfBoundsException ee){
			ee.printStackTrace();
		}
		
		//scheme2:substring(fromindex,toindex) method;setLength(int len);delete(fromindex,toindex) method + indexOf() method;
	 
	}
}

