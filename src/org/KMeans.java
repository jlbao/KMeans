package org;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.Test.Point;
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
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
		ArrayList<Point> centroids;
		
		@Override 
		public void setup(Context context){
			//get centroids from cache file
			 try
	            {
				 	centroids = new ArrayList<Point>();
					Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
					if(caches == null || caches.length <= 0){
						System.exit(1);
					}
					BufferedReader br = new BufferedReader(new FileReader(caches[0].toString()));
					String line;
					while((line = br.readLine()) != null){
						Point centroid = new Point(line);
						centroids.add(centroid);   
					}
	            }catch(Exception e){
	            	e.printStackTrace();
	            }
		}
		
		// map output: key is the nearest centroid, value is the point
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
				
				Point p = new Point(value.toString());
				Point nearestCentroid = p.getNearestCentroid(centroids);
				context.write(new Text(nearestCentroid.toString()), new Text(p.toString()));
			}
		}
		
	public static class Combine extends Reducer<Text, Text, Text, Text> {
		// the key of the combiner output is centroid, 
		// the value of the output is the count of the nearest points and the sum x and y
		// format (key, value) is like (centroid, (sum, totalX, totalY))
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			int count = 0;
			double totalX = 0;
			double totalY = 0;
			while(iter.hasNext()){
				String str = iter.next().toString();
				Point p = new Point(str);
				totalX += p.x;
				totalY += p.y;
				count++;
			}	
			context.write(key, new Text(count + "," + new Point(totalX, totalY).toString()));
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			int count = 0;
			double totalX = 0;
			double totalY = 0;
			while(iter.hasNext()){
				String str = iter.next().toString();
				String[] s = str.split(",");
				count += Integer.parseInt(s[0]);
				totalX += Integer.parseInt(s[1]);
				totalY += Integer.parseInt(s[2]);
			}

			Point newCentroid = new Point(totalX / count, totalY / count);
			context.write(new Text(newCentroid.toString()), key);
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