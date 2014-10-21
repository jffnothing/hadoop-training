package org.zkpk.hadoop.day0812.ex;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class IdentityReducer<K,V> extends Reducer<K, V, K, V> {

	@Override
	protected void reduce(K key, Iterable<V> values,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(V value:values){
			context.write(key, value);
		}
	}
}
