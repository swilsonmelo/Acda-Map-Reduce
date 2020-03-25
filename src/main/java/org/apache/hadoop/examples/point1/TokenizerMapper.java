package org.apache.hadoop.examples.point1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.gson.JsonObject;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private ArrayList<String> words;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		words = new ArrayList<String>(
		        Arrays.asList("TRUMP", "DICTATOR", "MAGA", "IMPEACH", "DRAIN", "SWAP", "CHANGE"));
		JSONParser parser = new JSONParser();
		JSONObject json;
		try {
			json = (JSONObject) parser.parse(value.toString());

			String text = json.get("text").toString();
			StringTokenizer itr = new StringTokenizer(text);
			while (itr.hasMoreTokens()) {
				String currentWord = itr.nextToken();
				currentWord = currentWord.toUpperCase();
				if( words.contains(currentWord) ) {
					word.set(currentWord);
					context.write(word, one);
				}				
			}
		} catch (ParseException e) {
			System.out.println("RIP");
			e.printStackTrace();
		}
	}

}