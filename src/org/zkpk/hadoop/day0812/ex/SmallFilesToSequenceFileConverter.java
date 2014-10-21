package org.zkpk.hadoop.day0812.ex;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SmallFilesToSequenceFileConverter extends Configured implements Tool {
	
	static class SequenceFileMapper extends Mapper<NullWritable,BytesWritable,Text,BytesWritable>{
		@Override
		protected void map(NullWritable key, BytesWritable value,
				Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			FileSplit fileSplit=(FileSplit)context.getInputSplit();
			String filename=fileSplit.getPath().getName();
			context.write(new Text(filename),value);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=NewJobBuilder.parseInputAndOutput(this,getConf(), args);
		if(job==null){
			return -1;
		}
		job.setJarByClass(SmallFilesToSequenceFileConverter.class);
		job.setMapperClass(SequenceFileMapper.class);
		job.setReducerClass(IdentityReducer.class);
		
//		job.setMapOutputKeyClass(NullWritable.class);
//		job.setMapOutputValueClass(BytesWritable.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new SmallFilesToSequenceFileConverter(),args);
		System.exit(exitCode);
	}

}
