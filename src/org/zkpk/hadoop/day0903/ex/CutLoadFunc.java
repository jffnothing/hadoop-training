package org.zkpk.hadoop.day0903.ex;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
//import org.eclipse.jetty.util.log.Log;
import org.mortbay.log.Log;

public class CutLoadFunc extends LoadFunc {
	//private static final Log LOG=LogFactory.getLog(CutLoadFunc.class);
	private final List<Range> ranges;
	private final TupleFactory tupleFactory=TupleFactory.getInstance();
	private RecordReader reader;
	
	public CutLoadFunc(String cutPattern){
		ranges=Range.parse(cutPattern);
	}
	@Override
	public void setLocation(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		FileInputFormat.setInputPaths(job, location);
	}

	@Override
	public InputFormat getInputFormat() throws IOException {
		// TODO Auto-generated method stub
		return new TextInputFormat();
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		// TODO Auto-generated method stub
		this.reader=reader;
	}

	@Override
	public Tuple getNext() throws IOException {
		// TODO Auto-generated method stub
		try {
			if(!reader.nextKeyValue()){
				return null;
			}
			Text value=(Text)reader.getCurrentValue();
			String line=value.toString();
			Tuple tuple=tupleFactory.newTuple(ranges.size());
			for(int i=0;i<ranges.size();i++){
				Range range=ranges.get(i);
				if(range.getEnd()>line.length()){
				//	LOG.warn(String.format("Range end (%s) is longer than line length (%s)",range.getEnd(),line.length()));
					Log.warn(String.format("Range end (%s) is longer than line length (%s)",range.getEnd(),line.length()));
					continue;
				}
				tuple.set(i,new DataByteArray(range.getSubstring(line)));
			}
			return tuple;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			throw new ExecException(e);
		}
	}
	@Override
	public LoadCaster getLoadCaster() throws IOException {
		// TODO Auto-generated method stub
		return super.getLoadCaster();
	}
	

}
