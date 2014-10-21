package org.zkpk.hadoop.day0901.ex;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class GenericUDFNvl extends GenericUDF {
	private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
	private ObjectInspector[] argumentOIs;
	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		// TODO Auto-generated method stub
		Object retVal=returnOIResolver.convertIfNecessary(arg0[0].get(),argumentOIs[0]);
		if(retVal==null){
			retVal=returnOIResolver.convertIfNecessary(arg0[1].get(),argumentOIs[1]);
		}
		return retVal;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		// TODO Auto-generated method stub
		StringBuilder sb=new StringBuilder();
		sb.append("if");
		sb.append(arg0[0]);
		sb.append("is null");
		sb.append("returns");
		sb.append(arg0[1]);
		return sb.toString();
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		// TODO Auto-generated method stub
		argumentOIs=arg0;
		if(arg0.length!=2){
			throw new UDFArgumentLengthException("The operator 'NVL' accepts 2 arguments.");
		}
		returnOIResolver=new GenericUDFUtils.ReturnObjectInspectorResolver(true);
		if(!(returnOIResolver.update(arg0[0])&& returnOIResolver.update(arg0[1]))){
			throw new UDFArgumentTypeException(2,"The 1st and 2nd args of function NVL should have same type,"+
					"but they are different:\""+arg0[0].getTypeName()
					+"\" and \""+arg0[1].getTypeName()+"\"");
		}
		return returnOIResolver.get();
	}

}
