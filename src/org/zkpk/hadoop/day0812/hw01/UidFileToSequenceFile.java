package org.zkpk.hadoop.day0812.hw01;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class UidFileToSequenceFile {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String arg0=args[0];
		Configuration conf0=new Configuration();
		FileSystem fs0=FileSystem.get(conf0);
		Path path=new Path(arg0);
		FSDataInputStream fsr;
		fsr=fs0.open(path);
		String line=fsr.readLine();
		
		String arg1=args[1];
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(arg1),conf);
		Path path1=new Path(arg1);
		NullWritable key=null;
		Text value=new Text();
		
		SequenceFile.Writer writer=null;
		writer=SequenceFile.createWriter(fs, conf, path1,key.getClass(), value.getClass());
		while(line!=null){
			value.set(line);
			writer.append(key, value);
			line=fsr.readLine();
		}
		IOUtils.closeStream(writer);
	}
}
