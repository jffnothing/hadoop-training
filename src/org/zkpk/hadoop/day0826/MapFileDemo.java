package org.zkpk.hadoop.day0826;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

public class MapFileDemo {
	public static void main(String[] args) throws Exception {
		read();
	}
	public static void read() throws Exception{
		String uri="hdfs://192.168.29.32:9000/output/826/result/";
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(uri),conf);
		Path inputPath=new Path(uri);
		
		MapFile.Reader reader=new MapFile.Reader(fs, uri, conf);
		Text key=(Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Text value=(Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		while(reader.next(key, value)){
			if(key.toString().equals("hbase.txt")){
				System.out.println(value);
			}
		}
	}
	
	public static void write() throws Exception{
		String uri="hdfs://192.168.29.32:9000/input/826/";
		String outputUri="hdfs://192.168.29.32:9000/output/826/result";
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(uri), conf);
		PathFilter pf=new MyPathFilter();
		
		MapFile.Writer writer=new MapFile.Writer(conf, fs, outputUri, Text.class, Text.class);
		Text key=new Text();
		Text value=new Text();
		FileStatus[] fss=fs.listStatus(new Path(uri), pf);
		for(FileStatus f:fss){
			byte[] buff=new byte[1024];
			
			FSDataInputStream in=fs.open(f.getPath());
			System.out.println(f.getPath().getName()+".....................");
			key.set(f.getPath().getName());
			int len=0;
			while((len=in.read(buff))!=-1){
				//System.out.println(len+"len................");
				//System.out.println(new String(buff));
				value.set(buff,0,len);
				writer.append(key, value);
				//System.out.println(buff.length+"bufflen............");
			}
			in.close();
		}
		
	}
}
class MyPathFilter implements PathFilter{

	public boolean accept(Path path) {
		return path.getName().endsWith(".txt");
	}
}