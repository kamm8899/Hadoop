package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMap1 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t",2); // Splits each line
		// If tab split doesn't work, try space split


		/**
		 *  TODO: read node-rank pair and emit: key:node, value:rank
		 */


		// Check if first section contains a colon (InitReducer format: node\trank:adj_list)
		// TODO: read node-rank pair and emit: key:node, value:rank
		String node;
		String rank;

		String left = sections[0].trim();
		String right = (sections.length > 1) ? sections[1] : "";

		if (right.contains(":")) {
			// Init output: node \t rank:adj_list
			int c = right.indexOf(':');
			node = left;
			rank = right.substring(0, c).trim();
		} else {
			// Iter output: "node rank" \t adj_list (possibly empty)
			String[] lr = left.split("\\s+", 2);
			if (lr.length < 2) {
				throw new IOException("Invalid node-rank format: " + line);
			}
			node = lr[0];
			rank = lr[1].trim();
		}


		// Validate that the rank is a valid double before emitting
		try {
			Double.parseDouble(rank);
			context.write(new Text(node), new Text(rank));
		} catch (NumberFormatException e) {
			throw new IOException("Invalid rank format for node '" + node + "': '" + rank + "' from line: " + line);
		}
	}
}