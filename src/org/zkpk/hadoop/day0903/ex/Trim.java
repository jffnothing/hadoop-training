package org.zkpk.hadoop.day0903.ex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class Trim extends EvalFunc<String> {

	@Override
	public String exec(Tuple tuple) throws IOException {
		// TODO Auto-generated method stub
		if(tuple==null || tuple.size()==0){
			return null;
		}
		Object object=tuple.get(0);
		if(object==null){
			return null;
		}
		String i=(String) object;
		return i.trim();
	}

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		// TODO Auto-generated method stub
		List<FuncSpec> funcList=new ArrayList<FuncSpec>();
		funcList.add(new FuncSpec(this.getClass().getName(),
				new Schema(new Schema.FieldSchema(null,DataType.CHARARRAY))));
		
		return funcList;
	}
	

}
