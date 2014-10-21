package org.zkpk.hadoop.day0917.ex;



import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HBaseAsInputResource {
	static final Log LOG=LogFactory.getLog(HBaseAsInputResource.class);
	public static final String NAME="Example Test1";
	public static final String TEMP_INDEX_PATH="/tmp/example";
	public static String inputTable="example";
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Configuration conf=HBaseConfiguration.create();
		Scan scan=new Scan();
		scan.setBatch(0);
		scan.setCaching(10000);
		scan.setMaxVersions();
		scan.setTimeRange(System.currentTimeMillis()-3*24*3600*1000L, System.currentTimeMillis());
		scan.addColumn(Bytes.toBytes("family1"), Bytes.toBytes("qualifier1"));
		conf.set("hbase.master","master:60000");
		//conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorm","master");
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		Path tempIndexPath=new Path(TEMP_INDEX_PATH);
		FileSystem fs=FileSystem.get(conf);
		if(fs.exists(tempIndexPath)){
			fs.delete(tempIndexPath,true);
		}
		Job job=new Job(conf,NAME);
		job.setJarByClass(HBaseAsInputResource.class);
		TableMapReduceUtil.initTableMapperJob(inputTable,scan,
				ExampleMapper.class,Text.class,Text.class,job);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, tempIndexPath);
		int success=job.waitForCompletion(true)?0:1;
		System.exit(success);
	}
	static class ExampleMapper extends TableMapper<Writable,Writable> {
		private Text k=new Text();
		private Text v=new Text();
		public static final String FIELD_COMMON_SEPARATOR="\u0001";
		@Override
		protected void map(ImmutableBytesWritable row, Result columns,Context context)
				throws IOException, InterruptedException {
			String value=null;
			String rowkey=new String(row.get());
			byte[] columnFamily=null;
			byte[] columnQualifier=null;
			long ts=0L;
			for(KeyValue kv:columns.list()){
				value=Bytes.toStringBinary(kv.getValue());
				columnFamily=kv.getFamily();
				columnQualifier=kv.getQualifier();
				ts=kv.getTimestamp();
				if("value1".equals(value)){
					k.set(rowkey);
					v.set(Bytes.toString(columnFamily)+FIELD_COMMON_SEPARATOR
						+Bytes.toString(columnQualifier)
						+FIELD_COMMON_SEPARATOR+value
						+FIELD_COMMON_SEPARATOR+ts
					);
					context.write(k, v);
					break;
				}
			}
			
		}
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		}
		
	}

}
