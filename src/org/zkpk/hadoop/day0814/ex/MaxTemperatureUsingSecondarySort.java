package org.zkpk.hadoop.day0814.ex;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTemperatureUsingSecondarySort extends Configured implements
		Tool {
	static class MaxTemperatureMapper extends Mapper<Object,Text,IntPair,NullWritable>{
		private NcdcRecordParser parser=new NcdcRecordParser();
		
		@Override
		public void map(Object key, Text value,Context context)throws IOException {
			// TODO Auto-generated method stub
			parser.parse(value);
			if(parser.isValidTemperature()){
				try {
					context.write(new IntPair(parser.getYearInt(),+parser.getAirTemperature()),NullWritable.get());
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	static class MaxTemperatureReducer extends Reducer<IntPair,NullWritable,IntPair,NullWritable>{

		@Override
		protected void reduce(IntPair key, Iterable<NullWritable> values,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key,NullWritable.get());
		}
		
	}
	public static class FirstPartitioner extends Partitioner<IntPair,NullWritable>{
		
		@Override
		public int getPartition(IntPair key, NullWritable value,
				int numPartitions) {
			// TODO Auto-generated method stub
			return Math.abs(key.getFirst().get()*127)%numPartitions;
		}
		
	}
	
	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(IntPair.class,true);
		}	
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			// TODO Auto-generated method stub
			IntPair ip1=(IntPair)w1;
			IntPair ip2=(IntPair)w2;
			int cmp=IntPair.compare(ip1.getFirst(), ip2.getFirst());
			//int cmp=ip1.getFirst().compareTo(ip2.getSecond());
			if(cmp!=0){
				return cmp;
			}
			return -IntPair.compare(ip1.getSecond(),ip2.getSecond());	
		}
	}
	public static class GroupComparator extends WritableComparator{
		protected GroupComparator(){
			super(IntPair.class,true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			// TODO Auto-generated method stub
			IntPair ip1=(IntPair)w1;
			IntPair ip2=(IntPair)w2;
			return IntPair.compare(ip1.getFirst(),ip2.getFirst());
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=NewJobBuilder.parseInputAndOutput(this, getConf(), args);
		if(job==null){
			return -1;
		}
		job.setJarByClass(MaxTemperatureUsingSecondarySort.class);
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setPartitionerClass(FirstPartitioner.class);
		//job.setOutputKeyComparatorClass(KeyComparator.class);¾É
		//ÐÂ
		job.setSortComparatorClass(KeyComparator.class);
		//job.setOutputValueGroupingComparator(GroupComparator.class);¾É
		//ÐÂ
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(IntPair.class);
		job.setOutputValueClass(NullWritable.class);
		
		return job.waitForCompletion(true)?0:1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new MaxTemperatureUsingSecondarySort(), args);
		System.exit(exitCode);
	}

}
