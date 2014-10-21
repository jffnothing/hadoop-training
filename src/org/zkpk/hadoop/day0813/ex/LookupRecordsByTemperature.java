package org.zkpk.hadoop.day0813.ex;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LookupRecordsByTemperature extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Path path=new Path(args[0]);
		IntWritable key=new IntWritable(Integer.parseInt(args[1]));
		FileSystem fs=path.getFileSystem(getConf());
		
		Reader[] readers=MapFileOutputFormat.getReaders(fs, path, getConf());
		Partitioner<IntWritable,Text> partitioner=new HashPartitioner<IntWritable,Text> ();
		Text val=new Text();
		
		Reader reader=readers[partitioner.getPartition(key, val, readers.length)];
		Writable entry=reader.get(key, val);
		if(entry==null){
			System.err.println("Key not found"+key);
			return -1;
		}
		NcdcRecordParser parser=new NcdcRecordParser();
		IntWritable nextKey=new IntWritable();
		do{
			parser.parse(val.toString());
			System.out.printf("%s\t%s\n",parser.getStationId(),parser.getYear());
		}while(reader.next(nextKey, val)&&key.equals(nextKey));
		
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new LookupRecordByTemperature(), args);
		System.exit(exitCode);
	}

}
