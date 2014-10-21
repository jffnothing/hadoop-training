package org.zkpk.hadoop.day0903.ex;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class IsGoodQuality extends FilterFunc{

	@Override
	public Boolean exec(Tuple tuple) throws IOException {
		// TODO Auto-generated method stub
		if(tuple==null || tuple.size()==0){
			return false;
		}
		Object object=tuple.get(0);
		if(object==null){
			return false;
		}
		int i=(Integer) object;
		return i==0 || i==1 || i==4 || i==5 || i==9;
	}
}
