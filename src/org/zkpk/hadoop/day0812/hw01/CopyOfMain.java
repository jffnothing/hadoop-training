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

public class CopyOfMain extends Configured implements Tool {
	
	static class TextMapper extends Mapper<Object,Text,Text,Text>{
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String [] arr=value.toString().split("\t",-1);
			if(arr.length==6){
				String uid=arr[1];
				String keyword=arr[2];
				context.write(new Text(uid),new Text(keyword));
			}
		}
	}
	static class SequenceMapper extends Mapper<LongWritable,Text,Text,Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(new Text(value.toString()),new Text("*"));
		}	
	}
	static class UidKeywordReducer extends Reducer<Text,Text,Text,Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			ArrayList<String> list=new ArrayList();
			boolean flag=false;
			for(Text value:values){
				if(value.toString().equals("*")){
					flag=true;
				}
				else{
					list.add(value.toString());
				}
			}
			if(flag){
				for(int i=0;i<list.size();i++){
					context.write(key,new Text(list.get(i)));
				}
			}
		}	
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=new Job();
		job.setJarByClass(CopyOfMain.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,TextMapper.class);
		MultipleInputs.addInputPath(job,new Path(args[1]),SequenceFileInputFormat.class,SequenceMapper.class);
		job.setReducerClass(UidKeywordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(10);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(String [] args) throws Exception{
		int exitCode=ToolRunner.run(new CopyOfMain(),args);
		System.exit(exitCode);
	}
}
