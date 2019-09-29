package WordCountMR.WordCountMR;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class WordCount {

	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tokenize = new StringTokenizer(value.toString());
			Text outKey = new Text();
			IntWritable outValue = new IntWritable(1);
			List<String> stopWords = new ArrayList<>(
					Arrays.asList("the", "i", "a", "de", "an", "en", "and", "this", "that","for","in","on","said"));
			while (tokenize.hasMoreTokens()) {
				String nextToken = tokenize.nextToken();
				if (!stopWords.contains(nextToken)) {

					outKey.set(nextToken);
					context.write(outKey, outValue);
				}

			}
		}
	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text term, Iterable<IntWritable> ones, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			Iterator<IntWritable> iterator = ones.iterator();
			while (iterator.hasNext()) {

				sum++;
				iterator.next();
			}
			IntWritable output = new IntWritable(sum);
			context.write(term, output);
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

		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(WordCount.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

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
