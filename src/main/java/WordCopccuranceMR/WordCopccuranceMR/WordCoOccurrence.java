package WordCopccuranceMR.WordCopccuranceMR;

import java.util.*;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCoOccurrence {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

		private MapWritable myMap = new MapWritable();
		private Text word = new Text();
		List<String> topTen = new ArrayList<>(Arrays.asList("like","election","would","people","report","campaign","US","vote","know","one"));

		// @Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			int neighbors = context.getConfiguration().getInt("neighbors", 2);
			String[] tokenized = value.toString().split("\\s+");
			if (tokenized.length > 1) {
				for (int i = 0; i < tokenized.length; i++) {

					if (topTen.contains(tokenized[i])) {
						getNeighbour(neighbors, tokenized, i);
						context.write(word, myMap);
					}
				}
			}
		}

		private void getNeighbour(int neighbors, String[] tokenized, int i) {

			int begin, end;
			word.set(tokenized[i]);
			myMap.clear();

			if (i - neighbors < 0) {
				begin = 0;
			} else {
				begin = i - neighbors;
			}
			if (i + neighbors >= tokenized.length) {
				end = tokenized.length - 1;
			} else {
				end = i + neighbors;
			}
			for (int j = begin; j <= end; j++) {
				if (j == i)
					continue;
				Text neighbor = new Text(tokenized[j]);
				if (myMap.containsKey(neighbor)) {
					IntWritable count = (IntWritable) myMap.get(neighbor);
					count.set(count.get() + 1);
				} else {
					myMap.put(neighbor, new IntWritable(1));
				}
			}
		}

	}

	public static class MyReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
		private MapWritable writeMap = new MapWritable();

		// @Override
		protected void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			writeMap.clear();
			for (MapWritable value : values) {
				collect(value);
			}
			context.write(key, writeMap);
		}

		private void collect(MapWritable mapWritable) {
			Set<Writable> keys = mapWritable.keySet();
			for (Writable key : keys) {
				IntWritable fromCount = (IntWritable) mapWritable.get(key);
				if (writeMap.containsKey(key)) {
					IntWritable count = (IntWritable) writeMap.get(key);
					count.set(count.get() + fromCount.get());
				} else {
					writeMap.put(key, fromCount);
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Invalue arguments : Use <Input_file>  <Output Dir>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "word Co occurance");

		job.setJarByClass(WordCoOccurrence.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		boolean status = job.waitForCompletion(true);

		if (status) {
			System.exit(0);
		}

		else {
			System.exit(1);
		}

	}

}
