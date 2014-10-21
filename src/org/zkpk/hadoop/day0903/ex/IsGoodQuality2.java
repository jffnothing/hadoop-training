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

public class IsGoodQuality2 extends EvalFunc<Boolean>{

	@Override
	public Boolean exec(Tuple tuple) throws IOException {
		// TODO Auto-generated method stub
		List<FuncSpec> list=getArgToFuncMapping();
		if(tuple==null || tuple.size()==0){
			return false;
		}
		Object object=list.get(0);
		if(object==null){
			return false;
		}
		int i=(Integer) object;
		return i==0 || i==1 || i==4 || i==5 || i==9;
	}

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		// TODO Auto-generated method stub
		List<FuncSpec> funcSpecs=new ArrayList<FuncSpec>();
		funcSpecs.add(new FuncSpec(this.getClass().getName(),
				new Schema(new Schema.FieldSchema(null,DataType.INTEGER))));
		
		return funcSpecs;
	}

}
