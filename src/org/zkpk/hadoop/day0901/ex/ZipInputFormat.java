package org.zkpk.hadoop.day0901.ex;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ZipInputFormat extends FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new ZipRecordReader();
	}
	
	
}

class ZipRecordReader extends RecordReader<LongWritable, Text> {
	private LongWritable key=new LongWritable();
	private Text value=new Text();
	private long currentPos;
	private boolean progress;
	private BufferedReader br;
	ByteArrayInputStream bis;
	ZipInputStream zip;
	private InputStream in;
	private ZipInputStream zis;
	private boolean flag;

	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		Path file = split.getPath();
		FileSystem fs = file.getFileSystem(job);
		File f = new File(file.toString());
		in = fs.open(file);
		byte[] data=new byte[in.available()];
		in.read(data);
		bis = new ByteArrayInputStream(data);
		zip = new ZipInputStream(bis);
		br = new BufferedReader(new InputStreamReader(zip));
		zip.getNextEntry();
		//unZip(data);
	}


	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		String line="";
		if((line=br.readLine())!=null){
			currentPos+=line.getBytes().length;
			key.set(currentPos);
			value.set(line);
			return true;
		}
		return false;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {

		return progress ? 0f : 1f;
	}

	@Override
	public void close() throws IOException {
		if(in!=null){
			in.close();
		}
		if(bis!=null){
			bis.close();
		}
		if(zip!=null){
			zip.close();
		}
		if(br!=null){
			br.close();
		}
		
		
	}
	

	public  boolean unZip(byte[] data) {
		try {
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return flag;
	}
	
	
	
}
