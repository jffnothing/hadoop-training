package org.zkpk.hadoop.day0820FirstTest.test3;



import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class NoMarketProvince extends Configured implements Tool {
	
	static class MarketMapper extends Mapper<Object,Text,Text,Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub 
			String [] arr=value.toString().split("\t",-1);//输入为3-1的输出
			if(arr.length==2){
				context.write(new Text(arr[0]),new Text("1"));
			}
		}
		
	}
	static class ProvinceMapper extends Mapper<Object,Text,Text,Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub   输入为省份
			context.write(value,new Text("2"));
		}
	}
	static class NoMarketReducer extends Reducer<Text,Text,Text,Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			boolean flag1=false;
			for(Text value:values){
				if(value.toString().equals("1")){
					flag1=true;
				}
			}
			if(!flag1){
				context.write(key,new Text(""));
			}
			
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=new Job();
		job.setJarByClass(NoMarketProvince.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,MarketMapper.class);
		MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,ProvinceMapper.class);
		job.setReducerClass(NoMarketReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		return job.waitForCompletion(true)?0:1;
	}
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new NoMarketProvince(),args);
		System.exit(exitCode);
	}

}
