package org.zkpk.hadoop.day0826;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

public class MapFileWriter{
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String [] data=new String[]{"","",""};
		Path [] path=new Path[]{new Path("d:\\cherry.txt"),new Path("d:\\hbase.txt"),new Path("d:\\printer.txt")};
		BufferedReader br1=new BufferedReader(new FileReader(path[0].toString()));
		BufferedReader br2=new BufferedReader(new FileReader(path[1].toString()));
		BufferedReader br3=new BufferedReader(new FileReader(path[2].toString()));
		BufferedReader [] br={br1,br2,br3};
		for(int i=0;i<3;i++){
			String line=br[i].readLine();
			while(line!=null){
				data[i]=data[i]+line+"\n";
				line=br[i].readLine();
			}
		}		
		String uri=args[0];
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(uri),conf);
		Text key=new Text();
		Text value=new Text();
		
		MapFile.Writer writer=null;
		writer=new MapFile.Writer(conf, fs, uri,Text.class,Text.class);
		
		for(int i=0;i<3;i++){
			key.set(path[i].getName());
			value.set(data[i].toString());
			writer.append(key, value);
		}
		IOUtils.closeStream(writer);	
	}
}
