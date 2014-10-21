package org.zkpk.hadoop.day0811.ex;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MinimalMain extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new MinimalMain(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		JobConf conf=JobBuilder.parseInputAndOutput(this,getConf(),args);
		if(conf==null){
			return -1;
		}
		conf.setJarByClass(MinimalMain.class);
		conf.setMapperClass(MinimalMapper.class);
		conf.setReducerClass(MinimalReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		JobClient.runJob(conf);
		return 0;
	}
}
