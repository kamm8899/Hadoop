package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TODO: emit key:node+rank, value: adjacency list
		 * Use PageRank algorithm to compute rank from weights contributed by incoming edges.
		 * Remember that one of the values will be marked as the adjacency list for the node.
		 */
		double d = PageRankDriver.DECAY; // Decay factor
		double rank = 0.0; // stores the decay factor in a variable rank

		String adjacencyList = "";
		//Process all values for this anode
		for (Text value : values) {
			String val = value.toString();

			if (val.startsWith("ADJ:")) {
				// This is the adjacency list
				adjacencyList = val.substring(4); // Extract adjacency list
			} else {
				// This is a rank contribution from an incoming edge
				double contribution = Double.parseDouble(val);
				rank += contribution; // Sum contributions
			}
		}
		double finalRank = (1 - d) + d * rank;

		String nodeWithRank = key.toString() + " " + finalRank;
		context.write(new Text(nodeWithRank), new Text(adjacencyList));
	}
}
