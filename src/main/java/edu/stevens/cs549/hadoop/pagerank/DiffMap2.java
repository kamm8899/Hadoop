package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMap2 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String s = value.toString(); // Converts Line to a String

		/*
		 * TODO: emit: key:"Difference" value:difference calculated in DiffRed1
		 */
		String[] sections = s.split("\t");
		if (sections.length >= 2) {
			// Extract the difference value (second part after the node)
			String difference = sections[1];

			// Emit with key "Difference" and the calculated difference as value
			context.write(new Text("Difference"), new Text(difference));
		}
	}
}
