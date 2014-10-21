package org.zkpk.hadoop.day0814.ex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextPair implements WritableComparable<TextPair> {
	private Text first;//id
	private Text second;//±ê¼Ç
	public TextPair(){
		set(new Text(),new Text());
	}
	public TextPair(String first,String second){
		set(new Text(first),new Text(second));
	}
	public TextPair(Text first,Text second){
		set(first,second);
	}
	public void set(Text first, Text second) {
		// TODO Auto-generated method stub
		this.first=first;
		this.second=second;
	}
	public Text getFirst(){
		return first;
	}
	public Text getSecond(){
		return second;
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
	public int compareTo(TextPair o) {
		// TODO Auto-generated method stub
		int cmp=first.compareTo(o.first);
		if(cmp!=0){
			return cmp;
		}
		return second.compareTo(o.second);
	}
	public static  int compare(Text first1,Text first2 ){
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
		if(obj instanceof TextPair){
			TextPair tp=(TextPair)obj;
			return first.equals(tp.first)&&second.equals(tp.second);
		}
		return false;
	}
	@Override
	public String toString() {
		return first + "\t" + second;
	}
	public static class FirstComparator extends WritableComparator{

		protected FirstComparator() {
			super(TextPair.class,true);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			TextPair tp1=(TextPair)a;
			TextPair tp2=(TextPair)b;
			return tp1.getFirst().compareTo(tp2.getFirst());
		}
	}
}
