package org;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KMeans extends Configured implements Tool{
	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {

			}
		}
		
	public static class Combine extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		}
	}
	
	public static class Reduce extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	
		}
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		
		return 0;
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        FileSystem fs=FileSystem.get(conf);
         
        Path dataFile = new Path("/input/initK");
        DistributedCache.addCacheFile(dataFile.toUri(), conf);
		
        Job job=new Job(conf);
        job.setJarByClass(KMeans.class);
        
        FileInputFormat.setInputPaths(job, "/input/points");
        Path outDir=new Path("/output/final");
        fs.delete(outDir,true);
        FileOutputFormat.setOutputPath(job, outDir);
         
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);
        //job.setOutputKeyClass(Point.class);
        //job.setOutputValueClass(Point.class);
         
        job.waitForCompletion(true);
	}


}