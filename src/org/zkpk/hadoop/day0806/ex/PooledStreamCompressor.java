package org.zkpk.hadoop.day0806.ex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.ReflectionUtils;

public class PooledStreamCompressor {

	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		String codecClassName=args[0];
		Class<?> codecClass=Class.forName(codecClassName);
		Configuration conf=new Configuration();
		CompressionCodec codec=(CompressionCodec)ReflectionUtils.newInstance(codecClass,conf);
		Compressor compressor=null;
		compressor=CodecPool.getCompressor(codec);
		CompressionOutputStream out;
		try {
			out = codec.createOutputStream(System.out, compressor);
			IOUtils.copyBytes(System.in, out, 4096, false);
			out.finish();
		}finally{
			CodecPool.returnCompressor(compressor);
		}
		
		
	}

}
