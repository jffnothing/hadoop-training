package org.zkpk.hadoop.day0812.hw01;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

public class IdentityMapper<V, K> extends Mapper<K, V, K, V> {

	@Override
	protected void map(K key, V value,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		context.write(key, value);
	}
}
