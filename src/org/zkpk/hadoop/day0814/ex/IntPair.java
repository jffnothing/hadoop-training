package org.zkpk.hadoop.day0814.ex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.zkpk.hadoop.day0807.ex.TextPair;


public class IntPair implements WritableComparable<IntPair> {
	private IntWritable first;
	private IntWritable second;
	public IntPair(){
		set(new IntWritable(),new IntWritable());
	}
	public IntPair(int first,int second){
		set(new IntWritable(first),new IntWritable(second));
	}
	public IntPair(IntWritable first,IntWritable second){
		set(first,second);
	}
	public void set(IntWritable first, IntWritable second) {
		// TODO Auto-generated method stub
		this.first=first;
		this.second=second;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		first.write(out);
		second.write(out);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		first.readFields(in);
		second.readFields(in);
	}
	@Override
	public int compareTo(IntPair o) {
		// TODO Auto-generated method stub
		int cmp=first.compareTo(o.first);
		if(cmp!=0){
			return cmp;
		}
		return second.compareTo(o.second);
	}
	public IntWritable getFirst() {
		// TODO Auto-generated method stub
		return first;
	}
	public IntWritable getSecond() {
		return second;
	}
	public void setSecond(IntWritable second) {
		this.second = second;
	}
	public void setFirst(IntWritable first) {
		this.first = first;
	}
	public static  int compare(IntWritable first1,IntWritable first2 ){
		int cmp=first1.compareTo(first2);
		return cmp;
	}
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return first.hashCode()*163 + second.hashCode();
	}
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if(obj instanceof IntPair){
			IntPair tp=(IntPair)obj;
			return first.equals(tp.first)&&second.equals(tp.second);
		}
		return false;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return first + "\t" + second;
	}
	
	
	
}
