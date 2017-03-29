package test1;




import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;


public class query3{
	public static class TokenizerMapperCustomers extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
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
		line.set(nvalues[0]);
		Text line1= new Text();
		line1.set("Cus"+","+nvalues[1]+","+nvalues[2]+","+nvalues[3]+","+nvalues[4]);
		output.collect(line,line1);
		}
		}
	
	public static class TokenizerMapperTrans extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
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
		line.set(nvalues[1]);
		Text line1= new Text();
		line1.set("Trans"+","+nvalues[0]+","+nvalues[2]+","+nvalues[3]+","+nvalues[4]);
		output.collect(line,line1);
		}
		}
	
	public static class ReducerJoin extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		private static  Text line=new Text();
		
		public void reduce(Text cId, Iterator<Text> value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			int num=0;
			int minItem=10;
			float totalSum=0;
			String[] out={"","","","",""};
			while(value.hasNext()){
				String values = value.next().toString();
				StringTokenizer token =new StringTokenizer(values,",");
				String[] nvalues={"","","","",""};
				int i=0;
				while(token.hasMoreTokens()){
					nvalues[i++]=token.nextToken().toString();
				}
				if(nvalues[0].equals("Cus")){
					out[0]=nvalues[1];
					out[1]=nvalues[4];
				}
				else if(nvalues[0].equals("Trans")){
					int item=Integer.parseInt(nvalues[3]);
					if(item<minItem){
						minItem=item;
					}
					totalSum+=Float.parseFloat(nvalues[2]);		
				}
				else{ System.out.print(false); }
				num++;
			}
			out[2]=Integer.toString(num);
			out[3]=Float.toString(totalSum);
			out[4]=Integer.toString(minItem);
			
			line.set(cId);
			Text lines=new Text();
			lines.set(out[0]+","+out[1]+","+out[2]+","+out[3]+","+out[4]);
			output.collect(line,lines);	
		}
	}
	 public static void main(String[] args) throws Exception {
			JobConf conf = new JobConf(query3.class);
			conf.setJobName("Query3");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setReducerClass(ReducerJoin.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, TokenizerMapperCustomers.class);
			MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, TokenizerMapperTrans.class);
			FileOutputFormat.setOutputPath(conf, new Path(args[2]));
			JobClient.runJob(conf);
		    }
	
	}

