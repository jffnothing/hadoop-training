package org.zkpk.hadoop.day0813.ex;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortByTemperatureToMapFile extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		JobConf conf=JobBuilder.parseInputAndOutput(this, getConf(), args);
		if(conf==null){
			return -1;
		}
		conf.setJarByClass(SortByTemperatureToMapFile.class);
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setNumReduceTasks(30);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputFormat(MapFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(conf,true);
		SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.BLOCK);
		JobClient.runJob(conf);
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new SortByTemperatureToMapFile(), args);
		System.exit(exitCode);
	}

}
