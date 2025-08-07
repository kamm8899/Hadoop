package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		/* 
		 * TODO: Just echo the input, since it is already in adjacency list format.
		 * Alternatively, output adjacency pairs that will be collected by reducer.
		 */
		
		String[] sections = line.split("\t"); // Splits it into two parts. Part 1: node;rank | Part 2: adj list
		if(parts.length >= 1){
			String node = sections[0];
			String adjList = sections.length > 1 ? sections[1] : ""; // Adjacency list, if present

			// Emit the node with its adjacency list
			context.write(new Text(node), new Text(adjList));
		} else {
			throw new IOException("Incorrect data format");
		}
	}

}
