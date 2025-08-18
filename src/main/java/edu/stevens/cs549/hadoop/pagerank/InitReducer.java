package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/*
		 * TODO: Output key: node+rank, value: adjacency list
		 */
		double initialRank = 1.0;

		StringBuilder adjList = new StringBuilder();
		for (Text val : values) {
			if (adjList.length() > 0) {
				adjList.append(" ");
			}
			adjList.append(val.toString());
		}
		context.write(key, new Text("1.0:" + adjList.toString()));
	}
}

