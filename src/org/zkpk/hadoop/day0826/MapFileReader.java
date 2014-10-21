package org.zkpk.hadoop.day0826;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class MapFileReader {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String mapuri=args[0];
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(mapuri),conf);
		Path map=new Path(mapuri);
		
		MapFile.Reader reader=new MapFile.Reader(fs,mapuri,conf);
		Text key=(Text)
				ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Text value=(Text)
				ReflectionUtils.newInstance(reader.getValueClass(), conf);
		while(reader.next(key, value)){
			if(key.toString().equals("hbase.txt")){
				System.out.println(value.toString());
			}
		}
	}
}
