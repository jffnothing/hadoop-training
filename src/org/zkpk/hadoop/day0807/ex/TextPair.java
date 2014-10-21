package org.zkpk.hadoop.day0807.ex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {
	private Text first;
	private Text second;
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
	
	public static void main(String [] args){
		TextPair text1=new TextPair("abc","abd");
		TextPair text2=new TextPair("abc","a");
		System.out.println(text1.getFirst().toString());
		System.out.println(text1.getSecond().toString());
		System.out.println(text1.toString());
		System.out.println(text1.equals(text2));
		System.out.println(text1.compareTo(text2));
	}
}
