package com.test.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * @Mapper<KEYIN , VALIN , KEYOUT ,VALOUT>
 * @author coco1
 *
 */
public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	// alt+/
	//每次调用map方法会传入split中的一行数据，key为该行数据所在文件中的位置下标，value为该行数据
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer st  = new StringTokenizer(line); //默认按空格切
		while( st.hasMoreTokens() ){
			String word = st.nextToken();
			context.write(new Text(word), new IntWritable(1)); //map的输出
		}
	}
}
