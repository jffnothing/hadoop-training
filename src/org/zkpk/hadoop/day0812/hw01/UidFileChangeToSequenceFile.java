package org.zkpk.hadoop.day0812.hw01;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UidFileChangeToSequenceFile extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=NewJobBuilder.parseInputAndOutput(this,getConf(), args);
		if(job==null){
			return -1;
		}
		job.setJarByClass(UidFileChangeToSequenceFile.class);
		job.setMapperClass(IdentityMapper.class);
		job.setReducerClass(IdentityReducer.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		return job.waitForCompletion(true)?0:1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new UidFileChangeToSequenceFile(),args);
		System.exit(exitCode);
	}

}
