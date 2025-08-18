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

		String[] sections = line.split(":"); // Splits it into two parts. Part 1: node;rank | Part 2: adj list
		if (sections.length >= 1) {
			String node = sections[0].trim();
			String adjList = sections.length > 1 ? sections[1].trim() : "";

			// Emit the node with its complete adjacency list as a single value
			context.write(new Text(node), new Text(adjList));
		} else {
			throw new IOException("Incorrect data format: " + line);
		}
	}

}
