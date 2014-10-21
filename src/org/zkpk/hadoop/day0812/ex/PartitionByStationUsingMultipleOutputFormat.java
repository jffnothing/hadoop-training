package org.zkpk.hadoop.day0812.ex;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class PartitionByStationUsingMultipleOutputFormat extends Configured implements Tool{

    static class StationNameMultipleTextOutputFormat extends MultipleTextOutputFormat<NullWritable,Text>{
    	private NcdcRecordParser parser=new NcdcRecordParser();
    	protected String generateFileNameForKeyValue(NullWritable key,Text value,String name){
		 parser.parse(value);
		 return parser.getStationId();
	    }
    }

    static class  StationMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{
	 private NcdcRecordParser parser=new NcdcRecordParser();

	 @Override
	 public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		parser.parse(value);
		output.collect(new Text(parser.getStationId()), value);
	 }
	 
    }
    static class StationReducer extends MapReduceBase implements Reducer<Text,Text,NullWritable,Text>{

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			while(values.hasNext()){
				output.collect(NullWritable.get(), values.next());
			}
		}
    	
    }
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		JobConf conf=JobBuilder.parseInputAndOutput(this, getConf(), args);
		if(conf==null){
			return -1;
		}
		conf.setMapperClass(StationMapper.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setReducerClass(StationReducer.class);
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputFormat(StationNameMultipleTextOutputFormat.class);
		
		JobClient.runJob(conf);
		return 0;
	}
	public static void main(String [] args) throws Exception{
		int exitCode=ToolRunner.run(new PartitionByStationUsingMultipleOutputFormat(), args);
		System.exit(exitCode);
	}

}

