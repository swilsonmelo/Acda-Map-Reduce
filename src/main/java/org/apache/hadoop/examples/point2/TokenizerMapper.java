package org.apache.hadoop.examples.point2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key,Text value, Context context
			) throws IOException, InterruptedException {
		String[] stopWords = {"a", "an", "and", "are", "etc"};
		String newValue = value.toString().replace("{", " ").replace("}", " ").replace("[", " ").replace("]", " ").replace("null", " ")
				.replace(",", " ");
		StringTokenizer itr = new StringTokenizer(newValue);		
		while (itr.hasMoreTokens()) {
			String token = itr.nextToken();
			boolean flag = false;
			for (String sW : stopWords) {
				if(token.equals(sW)) flag = true;
			}
			if( !flag) {
				word.set(token);
				context.write(word, one);
			}
		}
	}
	
}