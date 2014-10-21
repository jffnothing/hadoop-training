package org.zkpk.hadoop.day0804.ex;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class MyPathFilter implements PathFilter {
	public MyPathFilter(String regex){
		this.regex=regex;
	}
	private final String regex;
	@Override
	public boolean accept(Path path) {
		// TODO Auto-generated method stub
		return !path.toString().matches(regex);
	}
	public static void main(String [] args) throws IOException{
		String uri=args[0];
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(uri),conf);
		FileStatus[] status=fs.globStatus(new Path("hdfs://192.168.17.128:9000/test/*"),new MyPathFilter("^.*/test/test$"));
		for(FileStatus d:status){
			System.out.println(d.getPath());
		}
	}
}
