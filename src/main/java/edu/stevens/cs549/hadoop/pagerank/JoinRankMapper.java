package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinRankMapper extends Mapper<LongWritable, Text, TextPair, Text> {

	private final Text outVal = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		final String line = value.toString().trim();
		if (line.isEmpty()) return;

		// We may see "<left>\t<right>" or sometimes just one token.
		String[] parts = line.split("\t", 2);
		String left  = parts[0].trim();
		String right = (parts.length == 2) ? parts[1].trim() : "";

		String node = null;
		String rank = null;

		// Case A: "node;rank" on the LEFT (most common in iter output)
		int semi = left.indexOf(';');
		if (semi >= 0) {
			node = left.substring(0, semi).trim();
			rank = left.substring(semi + 1).trim();
		}

		// Case B: "node\trank" (simple pair)
		if (node == null) {
			if (!right.isEmpty() && isDouble(right)) {
				node = left;
				rank = right;
			}
		}

		// Case C: "node rank" on the LEFT
		if (node == null) {
			String[] lr = left.split("\\s+", 2);
			if (lr.length == 2 && isDouble(lr[1])) {
				node = lr[0].trim();
				rank = lr[1].trim();
			}
		}

		// Case D: "node\trank:adj_list" (rank before colon)
		if (node == null && !right.isEmpty()) {
			int colon = right.indexOf(':');
			if (colon > 0) {
				String maybeRank = right.substring(0, colon).trim();
				if (isDouble(maybeRank)) {
					node = left;
					rank = maybeRank;
				}
			}
		}

		// If we still couldn't parse, skip this line (or you can throw to catch bad inputs early)
		if (node == null || rank == null) {
			// context.getCounter("JoinRankMapper", "SkippedMalformed").increment(1);
			return;
		}

		// Emit key: (node, "1"), value: rank
		context.write(new TextPair(node, "1"), new Text(rank));
	}

	private static boolean isDouble(String s) {
		if (s == null || s.isEmpty()) return false;
		try {
			Double.parseDouble(s);
			return true;
		} catch (NumberFormatException nfe) {
			return false;
		}
	}
}
