package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class FinMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		/*
		 * TODO output key:-rank, value: node
		 * See IterMapper for hints on parsing the output of IterReducer.
		 */
		Counter processed = context.getCounter("FinMapper", "ProcessedLines");
		Counter emitted = context.getCounter("FinMapper", "EmittedLines");
		processed.increment(1);

		if (line.isEmpty()) return;

		String[] sections = line.split("\t");
		if (sections.length < 2) return;

		String vertexName = sections[0].trim();
		String rankStr = sections[1].trim();
		System.out.println("DEBUG: Processing line: " + vertexName + " " + rankStr);

		try {
			double rank = Double.parseDouble(rankStr);
			context.write(new DoubleWritable(-rank), new Text(vertexName));
			emitted.increment(1);
			System.out.println("DEBUG: Emitted: " + vertexName + " " + rank);
		} catch (NumberFormatException e) {
			System.err.println("ERROR: Invalid rank format for line: " + line);
		}
	}
}