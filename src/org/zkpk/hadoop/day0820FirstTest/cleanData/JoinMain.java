package org.zkpk.hadoop.day0820FirstTest.cleanData;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class JoinMain extends Configured implements Tool  {
	static class MarketMapper extends Mapper<Object,Text,Text,Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] arr=value.toString().split("\t");
			if(arr.length==3){
				context.write(new Text(arr[0]),new Text(arr[1]+"\t"+arr[2]));
			}
		}
	}
	static class RecordMapper extends Mapper<Object,Text,Text,Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] arr=value.toString().split("\t");
			if(arr.length==8){
				context.write(new Text(arr[7]), value);
			}
		}
	}
	static class JoinReducer extends Reducer<Text,Text,Text,NullWritable>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			String provincecity="";
			ArrayList<String> record=new ArrayList();
			for(Text value:values){
				String [] arr=value.toString().split("\t");
				if(arr.length==2){
					provincecity=value.toString();
				}
				else if(arr.length==8){//if day is 5 then length=7,else if day is 6 then length=8
					record.add(value.toString());
				}
			}
			if(!provincecity.equals("")){
				for(String str:record){
					context.write(new Text(str+"\t"+provincecity), NullWritable.get());
				}
			}
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=new Job();
		job.setJarByClass(JoinMain.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,MarketMapper.class);
		MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,RecordMapper.class);
		job.setReducerClass(JoinReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new JoinMain(),args);
		System.exit(exitCode);
	}
}
