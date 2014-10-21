package org.zkpk.hadoop.day0815.ex;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTemperatureByStationNameUsingDistributedCacheFile extends
		Configured implements Tool {
	static class StationTemperatureMapper extends Mapper<Object,Text,Text,IntWritable>{
		private NcdcRecordParser parser=new NcdcRecordParser();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			parser.parse(value);
			if(parser.isValidTemperature()){
				context.write(new Text(parser.getStationId()),new IntWritable(parser.getAirTemperature()));
			}
		}
		
	}
	static class MaxTemperatureReducerWithStationLookup extends Reducer<Text,IntWritable,Text,IntWritable>{
		private NcdcStationMetadata metadata;
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			metadata=new NcdcStationMetadata();
			metadata.initialize(new File(cacheFiles[0].toString()));
		}

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			String stationName=metadata.getStationName(key.toString());
			int maxValue=Integer.MIN_VALUE;
			for(IntWritable value:values){
				maxValue=Math.max(maxValue,value.get());
			}
			context.write(new Text(stationName), new IntWritable(maxValue));
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		DistributedCache.addCacheFile(new Path("/stations.txt").toUri(), conf);
		Job job=NewJobBuilder.parseInputAndOutput(this, conf, args);
		if(job==null){
			return -1;
		}
		job.setJarByClass(MaxTemperatureByStationNameUsingDistributedCacheFile.class);
		job.setMapperClass(StationTemperatureMapper.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducerWithStationLookup.class);
		
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
		int exitCode=ToolRunner.run(new MaxTemperatureByStationNameUsingDistributedCacheFile(), args);
		System.exit(exitCode);
	}

}
