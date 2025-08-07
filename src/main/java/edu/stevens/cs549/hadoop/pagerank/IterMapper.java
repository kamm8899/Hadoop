package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits it into two parts. Part 1: node;rank | Part 2: adj list

		if (sections.length > 2) // Checks if the data is in the incorrect format
		{
			throw new IOException("Incorrect data format");
		}
		if (sections.length != 2) {
			return;
		}
		
		/* 
		 * TODO: emit key: adj vertex, value: computed weight.
		 * 
		 * Remember to also emit the input adjacency list for this node!
		 * Put a marker on the string value to indicate it is an adjacency list.
		 */

		String nodeAndRank = sections[0];
		String adjList = sections[1];

		String[] nodeRankParts = nodeAndRank.split(":");
		if(nodeRankParts.length != 2){
			return;
		}
		String currentNode = nodeRankParts[0];
		double currentRank = Double.parseDouble(nodeRankParts[1]);

		//Emit the adjacency list with a marker

		context.write(new Text(currentNode), new Text("Adjacency List :" + adjList));

		//Parse adjacency List and distribute rank
		if(!adjList.isEmpty()) {
			String[] adjNodes = adjList.split(",");
			double weight = currentRank / adjNodes.length;

			for (String adjNode : adjNodes) {
				if (!adjNode.trim().isEmpty()) {
					// Emit the adjacent node with the computed weight
					context.write(new Text(adjNode), new Text(String.valueOf(weight)));
				}

			}
		}

}
