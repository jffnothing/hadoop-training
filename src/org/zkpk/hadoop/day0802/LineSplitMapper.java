package org.zkpk.hadoop.day0802;



import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LineSplitMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Text uid = new Text();
	private IntWritable one = new IntWritable(1);

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] arr = value.toString().split("\t", -1);
		if (arr.length == 6) {
			uid.set(arr[1]);
			context.write(uid, one);
		}
	}
}
