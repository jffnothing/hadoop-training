package org.zkpk.hadoop.day0812.hw01;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {
	static ArrayList<String> list1=new ArrayList();
	static ArrayList<String> list2=new ArrayList();
	
	static class TextMapper extends Mapper<Object,Text,Text,Text>{
		private Text mark1=new Text("mark1");
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String [] arr=value.toString().split("\t",-1);
			if(arr.length==6){
				String uid=arr[1];
				String keyword=arr[2];
				context.write(mark1,new Text(uid+"\t"+keyword));
			}
		}
	}
	static class SequenceMapper extends Mapper<LongWritable,Text,Text,Text>{
		private Text mark2=new Text("mark2");
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(mark2, value);
		}	
	}
	static class UidKeywordReducer extends Reducer<Text,Text,Text,Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if(key.toString().equals("mark2")){
				for(Text value:values){
					list1.add(value.toString());
				}
			}
			else if(key.toString().equals("mark1")){
				for(Text value:values){
					String [] arr=value.toString().split("\t",-1);
					if(arr.length==2){
						list2.add(value.toString());
					}
				}
			}
		}
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			for(int i=0;i<list2.size();i++){
				String [] arr=list2.get(i).split("\t",-1);
				String uid=arr[0];
				String keyword=arr[1];
				for(int j=0;j<list1.size();j++){
					if(uid.equals(list1.get(j))){
						context.write(new Text(uid),new Text(keyword));
					}
				}
			}
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=new Job();
		job.setJarByClass(Main.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,TextMapper.class);
		MultipleInputs.addInputPath(job,new Path(args[1]),SequenceFileInputFormat.class,SequenceMapper.class);
		job.setReducerClass(UidKeywordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(String [] args) throws Exception{
		int exitCode=ToolRunner.run(new Main(),args);
		System.exit(exitCode);
	}
}
