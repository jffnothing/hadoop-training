package org.zkpk.hadoop.day0806.ex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class StreamCompressor {

	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, IOException {
		// TODO Auto-generated method stub
			String codecClassName=args[0];
			Class<?> codecClass=Class.forName(codecClassName);
			Configuration conf=new Configuration();
			CompressionCodec codec=(CompressionCodec)ReflectionUtils.newInstance(codecClass,conf);
			CompressionOutputStream out=codec.createOutputStream(System.out);
			IOUtils.copyBytes(System.in, out,4096,false);
			out.finish();
	}

}
