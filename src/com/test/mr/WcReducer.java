package com.test.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text key, Iterable<IntWritable> iterable , //看到Iter就是看到了集合
			Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum = 0;
		for(IntWritable i : iterable){
			sum = sum + i.get();
		}
		context.write(key , new IntWritable(sum));
	}

}
