package org;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

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

/*
 * KMeans algorithm
 * Can iterate both according to iteration times and threshold
 */


public class KMeans_task3 extends Configured implements Tool{
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
		HashMap<Point, String> centroids; // key is the centroid, value is the sequence number
		@Override 
		public void setup(Context context){
			//get centroids from cache file
			 try{
				centroids = new HashMap<Point, String>();
				Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				if(caches == null || caches.length <= 0){
					System.exit(1);
				}
				BufferedReader br = new BufferedReader(new FileReader(caches[0].toString()));
				String line;
				while((line = br.readLine()) != null){
					String[] data = line.split(":");
					Point centroid = new Point(data[1]);// data[0] is the sequence number, data[1] is all the dimensions
					centroids.put(centroid, data[0]);   
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
				// put the sequence number before the old centroid
				context.write(new Text(centroids.get(nearestCentroid) + ":" + nearestCentroid.toString()), new Text(p.toString()));
			}
		}
		
	public static class Combine extends Reducer<Text, Text, Text, Text> {
		// the key of the combiner output is centroid, 
		// the value of the output is the count of the nearest points and the sum x and y
		// format (key, value) is like (centroid, (count; totalX, totalY, totalZ ...))
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			int count = 0;
			ArrayList<Double> list = new ArrayList<Double>();
			while(iter.hasNext()){
				String str = iter.next().toString();
				Point p = new Point(str);
				if(list.size() == 0){
					for(int i = 0; i < p.list.size(); i++){
						list.add(p.list.get(i));
					}	
				}else{
					for(int i = 0; i < p.list.size(); i++){
						list.set(i, list.get(i) + p.list.get(i));
					}
				}
				count++;
			}	
			context.write(key, new Text(count + ";" + new Point(list).toString()));
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			int count = 0;
			ArrayList<Double> total = new ArrayList<Double>();
			while(iter.hasNext()){
				String str = iter.next().toString();
				String[] s = str.split(";");
				count += Integer.parseInt(s[0]);
				Point p = new Point(s[1]);
				if(total.size() == 0){
					for(int i = 0; i < p.list.size(); i++){
						total.add(p.list.get(i));
					}	
				}else{
					for(int i = 0; i < p.list.size(); i++){
						total.set(i, total.get(i) + p.list.get(i));
					}
				}
			}

			for(int i = 0; i < total.size(); i++){
				total.set(i, total.get(i) / count);
			}
			
			Point newCentroid = new Point(total);
			context.write(key, new Text(newCentroid.toString()));
		}
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		return 0;
	}
	
	public static void jobConfig(Job job) throws Exception{
		job.setJarByClass(KMeans_task3.class);
		
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
	
	// this is used to decide if we still need another iteration based on the new centroid and the old centroid
	static boolean needIteration() throws Exception{
		File file = new File(Config.getNewCentroidPath());
		if(!file.exists())
			throw new Exception("newCentroids file does not exist!");
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = "";
		while((line = reader.readLine()) != null){
			String[] str = line.split("\t");
			Point prevPoint = new Point(str[0].split(":")[1]);
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
		HashMap<Point, String> table = new HashMap<Point, String>(); // key is the point, value is the sequence number
		String line = "";
		while((line = reader.readLine()) != null){
			String[] str = line.split("\t");
			String[] s = str[0].split(":"); // parse the sequence number, which is str[0]
			Point newPoint = new Point(str[1]);
			table.put(newPoint, s[0]);
		}
		reader.close();
		
		// update the newCentroid file
		
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		ArrayList<String> outputList = new ArrayList<String>();
			
		Set<Entry<Point,String>> set = table.entrySet();
		
		for(Entry<Point,String> entry : set){
			outputList.add(entry.getValue() + ":" + entry.getKey().toString());
			//bw.write(p.toString() + "\n");
		}
		
		for(int i = 0; i < outputList.size() - 1; i++){
			bw.write(outputList.get(i) + "\n");
		}
		
		bw.write(outputList.get(outputList.size() - 1));
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
			// make configuration(load configuration from config.xml)
			Config.makeConfiguration();
			
			int i = 0;
			int iterationCount = getIterationCount();
			boolean iterationFlag = true;
			while(i < iterationCount && iterationFlag){
				//set up the current job configuration
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(conf);	
				setupData(conf, fs, i);
				
				// run job
				Job job = new Job(conf, "KMeans_task3");
				KMeans_task3.jobConfig(job);
				job.waitForCompletion(true);
				
				// copy the output file to local file system
				fs.copyToLocalFile(new Path("/output/part-r-00000"), new Path(Config.getNewCentroidPath()));
				
				// see if this should be stopped by distance or iteration count
				if(!Config.getIsStopIterationByCount() && !needIteration()){
					iterationFlag = false;
				}
				
				// update the newCentroid data file
				updateNewCentroidFile();
		        i++;
			}
		}catch(Exception e){
			e.printStackTrace();
		}
        
	}
}