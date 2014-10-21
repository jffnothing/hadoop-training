package org.zkpk.hadoop.day0814.ex;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortByTemperatureUsingTotalOrderPartitioner extends Configured
		implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		//若不写这句，直接在下边写Job job=NewJobBuilder.parseInputAndOutput(this,getConf, args);会报错，说can't read partitions file
		Job job=NewJobBuilder.parseInputAndOutput(this,conf, args);
		if(job==null){
			return -1;
		}
		job.setJarByClass(SortByTemperatureUsingTotalOrderPartitioner.class);
		job.setNumReduceTasks(30);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job,true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		
		//开始用TotalOrderPartitioner排序
		job.setPartitionerClass(TotalOrderPartitioner.class);
		//采样器
		InputSampler.Sampler<IntWritable,Text> sampler=new InputSampler.RandomSampler<IntWritable,Text>(0.1,10000,10);
		
		Path input=FileInputFormat.getInputPaths(job)[0];
		input=input.makeQualified(input.getFileSystem(job.getConfiguration()));
		
		Path partitionFile=new Path(input,"_partitions");
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),partitionFile);
		InputSampler.writePartitionFile(job, sampler);
		
		URI partitionUri=new URI(partitionFile.toString()+"#_partitions");
		DistributedCache.addCacheFile(partitionUri,job.getConfiguration());//addCacheFile(partitionUri,job);
		DistributedCache.createSymlink(job.getConfiguration());
		
		return job.waitForCompletion(true)?0:1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new SortByTemperatureUsingTotalOrderPartitioner(), args);
		System.exit(exitCode);
	}

}
