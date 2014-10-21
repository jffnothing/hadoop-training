package org.zkpk.hadoop.day0901.ex;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void main(String[] args) throws Exception {
		Job job=new Job(new Configuration());
		
		job.setJarByClass(WordCount.class);
		job.setInputFormatClass(ZipInputFormat.class);
		ZipInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.waitForCompletion(true);
	}

}

class WordCountMapper extends Mapper<LongWritable,Text,NullWritable,Text>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
	
		context.write(NullWritable.get(), value);
	}
	
}
class WordCountReducer extends Reducer<NullWritable,Text,NullWritable,Text>{

	@Override
	protected void reduce(NullWritable arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
		for(Text v:arg1){
			arg2.write(NullWritable.get(), v);
		}
	}
}

