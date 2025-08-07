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
		String[] sections = line.split("\t"); // Splits it into two parts. Part 1: node;rank | Part 2: adj list
		if (sections.length == 2) {
			String node = sections[0];
			double rank = Double.parseDouble(sections[1]);

			//Output key: -rank, value: node
			context.write(new DoubleWritable(-rank), new Text(node));
		}
		}

	}

}
