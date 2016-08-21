package com.test.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class JobRun {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "master:9001");
		try {
			Job job =  new Job(conf);
			job.setJarByClass(JobRun.class);
			job.setMapperClass(WcMapper.class);
			job.setReducerClass(WcReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setNumReduceTasks(1);
			FileInputFormat.setInputPaths(job , new Path("hdfs://192.168.1.30:9000/usr/input/wc"));
			FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.30:9000/usr/output/wc"));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
