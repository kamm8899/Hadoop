package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {

		// Expected: "<node;rank>\t<adjacency list>"
		// (Also accept "node rank" on the left side.)
		final String line = value.toString();
		final String[] sections = line.split("\t", 2);

		if (sections.length != 2) {
			throw new IOException(
					"IterMapper: Incorrect data format. Expected two sections separated by tab. Line: " + line
			);
		}

		/*
		 * TODO: emit key: adj vertex, value: computed weight.
		 *
		 * Remember to also emit the input adjacency list for this node!
		 * Put a marker on the string value to indicate it is an adjacency list.
		 */

		final String leftRaw  = sections[0].trim();
		final String rightRaw = sections[1].trim();

		String node = null;
		String rankStr = null;
		String adjList = rightRaw;  // may be overwritten if right is "rank:adj_list"

		// Case 1: "node;rank" on the left
		int semi = leftRaw.indexOf(';');
		if (semi >= 0) {
			node = leftRaw.substring(0, semi).trim();
			rankStr = leftRaw.substring(semi + 1).trim();
		} else {
			// Case 2: "node rank" on the left
			String[] lr = leftRaw.split("\\s+", 2);
			if (lr.length == 2) {
				node = lr[0].trim();
				rankStr = lr[1].trim();
			} else {
				// Case 3: "node" on the left AND "rank:adj_list" on the right
				int colon = rightRaw.indexOf(':');
				if (colon > 0) {
					node = leftRaw;
					rankStr = rightRaw.substring(0, colon).trim();
					adjList = rightRaw.substring(colon + 1).trim();
				} else {
					throw new IOException("IterMapper: cannot parse node/rank from line: " + line);
				}
			}
		}

		final double rank;
		try {
			rank = Double.parseDouble(rankStr);
		} catch (NumberFormatException nfe) {
			throw new IOException("IterMapper: non-numeric rank '" + rankStr + "' in line: " + line);
		}

		// Forward the adjacency list with a marker so the reducer can recognize it.
		context.write(new Text(node), new Text("ADJ:" + adjList));

		// Emit PageRank contributions to neighbors.
		if (!adjList.isEmpty()) {
			String[] neighbors = adjList.split("[,\\s]+"); // support spaces and/or commas
			int deg = 0;
			for (String v : neighbors) {
				if (!v.isEmpty()) deg++;
			}
			if (deg > 0) {
				String contrib = Double.toString(rank / deg);
				for (String v : neighbors) {
					v = v.trim();
					if (!v.isEmpty()) {
						context.write(new Text(v), new Text(contrib));
					}
				}
			}
		}
	}
}