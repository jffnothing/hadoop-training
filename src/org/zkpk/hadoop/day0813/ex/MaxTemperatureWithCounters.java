package org.zkpk.hadoop.day0813.ex;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MaxTemperatureWithCounters extends Configured implements Tool {
	enum Temperature{
		MISSING,
		MALFORMED
	}
	static class MaxTemperatureMapperWithCounters extends Mapper<LongWritable,Text,Text,IntWritable>{
		private NcdcRecordParser parser=new NcdcRecordParser();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			parser.parse(value);
			if(parser.isValidTemperature()){
				int airTemperature=parser.getAirTemperature();
				context.write(new Text(parser.getyear()),new IntWritable(parser.getAirTemperature()));
			}else if(parser.isMalformedTemperature()){
				System.err.println("Ignoring possibly corrupt input:"+value);
				context.getCounter(Temperature.MALFORMED).increment(1);
			}else if(parser.isMissingTemperature()){
				context.getCounter(Temperature.MISSING).increment(1);
				//等于就API中的report.incrCounter(Temperature.MISSING,1);
			}
			context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=NewJobBuilder.parseInputAndOutput(this, getConf(), args);
		if(job==null){
			return -1;
		}
		job.setJarByClass(MaxTemperatureWithCounters.class);
		job.setMapperClass(MaxTemperatureMapperWithCounters.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true)?0:1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new MaxTemperatureWithCounters(), args);
		System.exit(exitCode);
	}

}
