package org;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
					br.close();
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
		// format (key, value) is like (centroid, (count, totalX, totalY))
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
			context.write(key, new Text(count + ";" + new Point(totalX, totalY).toString()));
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
				String[] s = str.split(";");
				count += Integer.parseInt(s[0]);
				s = s[1].split(",");
				totalX += Double.parseDouble(s[0]);
				totalY += Double.parseDouble(s[1]);
			}

			Point newCentroid = new Point(totalX / count, totalY / count);
			context.write(key, new Text(newCentroid.toString()));
		}
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		return 0;
	}
	
	public static void jobConfig(Job job) throws Exception{
		job.setJarByClass(KMeans.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// design mapper, combiner and reducer
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		
		// in order to have one file as output
		job.setNumReduceTasks(1);
		
		// set input and output paths
		FileInputFormat.setInputPaths(job, new Path("/input/points"));
		FileOutputFormat.setOutputPath(job, new Path("/output")); 
		
		// set input and output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	}
	
	// this is used to decide if we still need another iternation based on the new centroid and the old centroid
	static boolean needIteration() throws Exception{
		File file = new File(Config.getNewCentroidPath());
		if(!file.exists())
			throw new Exception("newCentroids file does not exist!");
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = "";
		while((line = reader.readLine()) != null){
			String[] str = line.split("\t");
			Point prevPoint = new Point(str[0]);
			Point newPoint = new Point(str[1]);
			if(Point.getDistance(prevPoint, newPoint) > Config.getStopIterationThreshold()){
				reader.close();
				return true;
			}
		}
		reader.close();
		return false;
	}
	
	// update the newCentroid file in order to be utilized for next iteration
	// this is used to update the new Centroid file so as the it can be put into the cache file
	static boolean updateNewCentroidFile() throws Exception{
		File file = new File(Config.getNewCentroidPath());
		if(!file.exists())
			throw new Exception("newCentroids file does not exist!");
		
		// set the file to be inexecutable
		file.setExecutable(false);
		
		BufferedReader reader = new BufferedReader(new FileReader(file));
		ArrayList<Point> list = new ArrayList<Point>();
		String line = "";
		while((line = reader.readLine()) != null){
			String[] str = line.split("\t");
			Point newPoint = new Point(str[1]);
			list.add(newPoint);
		}
		reader.close();
		
		// update the newCentroid file
		
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		for(int i = 0 ; i < list.size() - 1; i++){
			Point p = list.get(i);
			bw.write(p.toString() + "\n");
		}
		bw.write(list.get(list.size() - 1).toString());
		bw.close();
		
		// delete the .crc file because FSInputChecker will check .crc file,
		// http://stackoverflow.com/questions/12310967/checksum-error-when-using-hdfs-copyfromlocalfile
		file = new File("data/.newCentroids.crc");
		return file.delete();
	}
	
	// set up the cache and config
	static void setupData(Configuration conf, FileSystem fs, int currentIterationTime) throws Exception{
		fs.delete(new Path("/output"), true);
		fs.copyFromLocalFile(new Path("data/points"), new Path("/input/points"));
		
		if(currentIterationTime == 0){ // if this is the first time to run KMeans
			fs.copyFromLocalFile(new Path(Config.getInitCentroidPath()), new Path("/cache/initCentroids"));
			DistributedCache.addCacheFile(new Path("/cache/initCentroids").toUri(), conf);
		}else{
			fs.copyFromLocalFile(new Path(Config.getNewCentroidPath()), new Path("/cache/newCentroids"));
			DistributedCache.addCacheFile(new Path("/cache/newCentroids").toUri(), conf);
		}
	}
	
	// get the iteration count if the condition is ended according to times
	static int getIterationCount(){
		if(Config.getIsStopIterationByCount())
			return Config.getIterationCount();
		else
			return Integer.MAX_VALUE;
	}
	
	public static void main(String[] args) {
		try{
			int i = 0;
			int iterationCount = getIterationCount();
			
			while(i < iterationCount){
				//set up the current job configuration
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(conf);	
				setupData(conf, fs, i);
				
				// run job
				Job job = new Job(conf, "KMeans");
				KMeans.jobConfig(job);
				job.waitForCompletion(true);
				
				// copy the output file to local file system
				fs.copyToLocalFile(new Path("/output/part-r-00000"), new Path(Config.getNewCentroidPath()));
				
				// see if this should be stopped by distance or iteration count
				if(!Config.getIsStopIterationByCount() && !needIteration())
					return;
				
				// update the newCentroid data file
				updateNewCentroidFile();
		        i++;
			}
		}catch(Exception e){
			e.printStackTrace();
		}
        
	}
}