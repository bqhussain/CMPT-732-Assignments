package assignment;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaPopular extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, IntWritable>{

//		private final static IntWritable one = new IntWritable(1);
		private IntWritable pageCountMax = new IntWritable();
		private Text dateTime = new Text();
		int count;
		String name, lang;

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			
			String[] WikiFile = value.toString().split(" ");
			
			dateTime.set(WikiFile[0]);
			lang = WikiFile[1];
			name = WikiFile[2];
			count= Integer.parseInt(WikiFile[3]);
			
			
			if( !name.equals("Main_Page") && !name.startsWith("Special:")  && lang.equals("en")) {
				pageCountMax.set(count);
				context.write(dateTime, pageCountMax);
			}
		}
	}

	public static class IntSumReducer
	extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int max = 0;
			for (IntWritable val : values) {
				if(val.get() > max){
				max = val.get();
				}
			}
			result.set(max);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "wiki max");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
