package org.zkpk.hadoop.day0812.hw02;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartitionByUidUsingMultipleOutputs extends Configured implements Tool {
	static class MultioleOutputMapper extends Mapper<LongWritable,Text,Text,Text>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String [] arr=value.toString().split("\t",-1);
			String uid=arr[1];
			context.write(new Text(uid), value);
		}
		
	}
	static class MultipleOutputsReducer extends Reducer<Text,Text,NullWritable,Text>{
		private  MultipleOutputs multipleOutputs;
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			for(Text value:values){
				multipleOutputs.write(NullWritable.get(), value,key.toString());
			}	
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			multipleOutputs.close();
		}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			multipleOutputs=new MultipleOutputs<NullWritable,Text>(context);
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=NewJobBuilder.parseInputAndOutput(this, getConf(), args);
		if(job==null){
			return -1;
		}
		job.setJarByClass(PartitionByUidUsingMultipleOutputs.class);
		job.setMapperClass(MultioleOutputMapper.class);
		job.setReducerClass(MultipleOutputsReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		return job.waitForCompletion(true)?0:1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new PartitionByUidUsingMultipleOutputs(), args);
		System.exit(exitCode);
	}

}
