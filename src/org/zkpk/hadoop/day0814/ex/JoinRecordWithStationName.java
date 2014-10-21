package org.zkpk.hadoop.day0814.ex;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinRecordWithStationName extends Configured implements Tool {
	
	static class JoinStationMapper extends Mapper<Object,Text,TextPair,Text>{
		private NcdcStationMetadataParser parser=new NcdcStationMetadataParser();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
//			String [] arr=value.toString().split("\t",-1);
//			if(arr.length==2){
//				context.write(new TextPair(arr[0], "0"),new Text(arr[1]));
//			}
			if(parser.parse(value)){
				context.write(new TextPair(parser.getStationId(),"0"),new Text(parser.getStationName()));
			}
		}
	}
	static class JoinRecordMapper extends Mapper<Object,Text,TextPair,Text>{
		private NcdcRecordParser parser=new NcdcRecordParser();

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			parser.parse(value);
			context.write(new TextPair(parser.getStationId(),"1"), value);
		}
	}
	
	static class JoinReducer extends Reducer<TextPair,Text,Text,Text>{

		@Override
		protected void reduce(TextPair key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Text stationName=new Text(values.iterator().next());
			while(values.iterator().hasNext()){
				Text record=values.iterator().next();
				Text outValue=new Text(stationName.toString()+"\t"+record.toString());
				context.write(key.getFirst(), outValue);
			}
		}
		
	}
	
	public static class KeyPartitioner extends Partitioner<TextPair,Text>{

		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			// TODO Auto-generated method stub
			return (key.getFirst().hashCode()& Integer.MAX_VALUE)%numPartitions;
		}
		
	} 
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=3){
			NewJobBuilder.printUsage(this, "<ncdc input> <station input> <output>");
			return -1;
		}
		Job job=new Job();
		job.setJarByClass(JoinRecordWithStationName.class);
		job.setJobName("Join record with station name");
		
		Path ncdcInputPath=new Path(args[0]);
		Path stationInputPath=new Path(args[1]);
		Path outputPath=new Path(args[2]);
		
		MultipleInputs.addInputPath(job, ncdcInputPath,TextInputFormat.class, JoinRecordMapper.class);
		MultipleInputs.addInputPath(job, stationInputPath, TextInputFormat.class, JoinStationMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setPartitionerClass(KeyPartitioner.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		return job.waitForCompletion(true)?0:1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new JoinRecordWithStationName(), args);
		System.exit(exitCode);
	}

}
