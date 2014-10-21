package org.zkpk.hadoop.day0818.ex;

import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.*;

public class MaxTemperatureMapperTest {

	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
		MaxTemperatureMapper mapper=new MaxTemperatureMapper();
		Text value=new Text("0043011990999991950051518004+68750+023550FM-12+0382"+"99999V0203201N00261220001CN9999999N9-00111+99999999999");
		//OutputCollector<Text,IntWritable> output=mock(OutputCollector.class);
		Context context=mock(Context.class);
		mapper.map(null, value,context);
		verify(context).write(new Text("1950"),new IntWritable(-11));
	}

}
