package org.zkpk.hadoop.day0917.ex;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseAsOutputResource extends Configured implements Tool {
	static final Log LOG = LogFactory.getLog(HBaseAsOutputResource.class);
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String input = "hdfs://192.168.32.128:9000/sogou/sogou2";
		String table = "sogou2";
		Configuration conf = HBaseConfiguration.create();
		// hbase master
		conf.set("hbase.master","master:60000");
		// zookeeper quorum
		conf.set("hbase.zookeeper.quorum", "master");
		Job job = new Job(conf, "Import from file " + input + " into table " + table);
		job.setJarByClass(HBaseAsOutputResource.class);
		job.setMapperClass(ExampleMapper.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Writable.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(input));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		int res = 1;
		try {
			res = ToolRunner.run(conf,new HBaseAsOutputResource(), otherArgs);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(res);
	}
	static class ExampleMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {
		static byte[] family =Bytes.toBytes("s1");
		static byte[] qualifier = null;
		static byte[] val = null;
		static String rowkey = null;
		String [] qualifiers=new String[]{"time","keyword","rank","order","url"};
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				String lineString = value.toString();
				String[] arr = lineString.split("\t", -1);
				if (arr.length == 6) {
					rowkey = arr[0];
					Put put = new Put(rowkey.getBytes());
					for(int i=0;i<qualifiers.length;i++){
						qualifier=Bytes.toBytes(qualifiers[i]);
						val=Bytes.toBytes(arr[i+1]);
						put.add(family, qualifier, val);
					}
					context.write(new ImmutableBytesWritable(rowkey.getBytes()), put);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}

}
