package org.zkpk.hadoop.day0807.ex;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

public class MapFileFixer {
	public static void main(String [] args) throws Exception{
		String mapuri=args[0];
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(mapuri),conf);
		Path map=new Path(mapuri);
		Path mapData=new Path(map,MapFile.DATA_FILE_NAME);
		
		SequenceFile.Reader reader=new SequenceFile.Reader(fs,mapData,conf);
		Class keyClass=reader.getKeyClass();
		Class valueClass=reader.getValueClass();
		reader.close();
		long entries=MapFile.fix(fs, map, keyClass, valueClass, false, conf);
		System.out.printf("Created MapFile %s with %d entries\n",map,entries);
	}
}
