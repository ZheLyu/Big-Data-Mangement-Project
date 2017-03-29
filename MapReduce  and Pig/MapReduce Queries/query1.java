package test1;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class query1 {
	public static class TokenizerMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
		private static Text line=new Text();
 
		public void map(LongWritable key, Text value, OutputCollector<Text,Text> output,Reporter reporter
		 ) throws IOException{
		
		String values = value.toString();
		StringTokenizer token =new StringTokenizer(values,",");
		String[] nvalues=new String[token.countTokens()];
		int i=0;
		while(token.hasMoreElements()){
			nvalues[i++]=token.nextToken().toString();
		}
		Integer countrycode=Integer.parseInt(nvalues[3]);
		
		if(countrycode>=2&&countrycode<=6){
		line.set(nvalues[0]+",");
		Text line1= new Text();
		line1.set(nvalues[1]+","+nvalues[2]+","+nvalues[3]+","+nvalues[4]);
		output.collect(line,line1);
		}
		}
	}
	public static void main(String[] args) throws Exception {
		
		JobConf query1 = new JobConf(query1.class);
		query1.setJobName("query1");
		query1.setMapperClass(TokenizerMapper.class);
		query1.setInputFormat(TextInputFormat.class);
		query1.setOutputFormat(TextOutputFormat.class);
		query1.setOutputKeyClass(Text.class);
		query1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(query1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(query1, new Path(args[1]));

		JobClient.runJob(query1);
	 
	  }
	}
	
	

