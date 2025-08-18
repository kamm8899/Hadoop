package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed1 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double[] ranks = new double[2];
		/*
		 * TODO: The list of values should contain two ranks.  Compute and output their difference.
		 */
		int index = 0;
		for (Text value : values) {
			if (index < 2) {
				// Since DiffMap1 now emits just the rank, parse directly
				ranks[index] = Double.parseDouble(value.toString());
				index++;
			}
		}

		// Check if we have exactly two ranks
		if (index == 2) {
			// Compute the difference between the two ranks
			double diff = Math.abs(ranks[0] - ranks[1]);

			// Output the key and the computed difference
			context.write(key, new Text(String.valueOf(diff)));
		}
	}
}