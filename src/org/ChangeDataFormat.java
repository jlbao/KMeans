package org;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map.Entry;

public class ChangeDataFormat {

	/**
	 * change the data format to make 
	 */
	
	static HashMap<Integer, Point> newCentroids; // key is the cluster ID, value is the centroid dimensions
	
	public static void main(String[] args) throws Exception{
		long startTime = System.currentTimeMillis();
		setNewCentroid();
		String output = "";
		for(Entry<Integer, Point> entry : newCentroids.entrySet()){
			Node[] res = getTop10NearestPoints(entry.getValue());
			output += entry.getKey() + ":";
			for(int i = 0; i < res.length - 1; i++){
				output += res[i].toString() + ",";
			}
			output += res[res.length - 1].toString() + "\n";
		}
		output.substring(0, output.length() - 1);
		
        File file = new File("results");
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(output);
		bw.close();
		
		long endTime = System.currentTimeMillis();
		
		System.out.println(endTime - startTime);
	}

	public static Node[] getTop10NearestPoints(Point center) throws Exception{
		Node[] res = new Node[10];
		for(int i = 0; i < res.length; i++){
			res[i] = new Node(0.0, "");
		}
		
		File pointFile = new File("points_doc");
		BufferedReader fr = new BufferedReader(new FileReader(pointFile));
		String inputLine;
		while((inputLine = fr.readLine()) != null){
			String[] data = inputLine.split("\t"); // data[0] is the doc id, data[1] is the the points information
			String docId = data[0];
			Point p = new Point(data[1]);
			double distance = Point.getDistance(p, center);
			int minIndex = 0;
			for(int i = 1; i < res.length; i++){ // always find the minimun to insert
				if(res[minIndex].distance > res[i].distance)
					minIndex = i;
			}
			res[minIndex] = new Node(distance, docId);
		}
		fr.close();
		return res;
	}
	
	
	
	public static void setNewCentroid() throws Exception{
		newCentroids = new HashMap<Integer, Point>();
		File pointFile = new File("newCentroids");
		BufferedReader fr = new BufferedReader(new FileReader(pointFile));
		
		String inputLine;
		while((inputLine = fr.readLine()) != null){
			String[] data = inputLine.split(":");
			int clusterId = Integer.parseInt(data[0]);
			Point p = new Point(data[1]);
			newCentroids.put(clusterId, p);
		}
		fr.close();
	}
	
	static class Node{
		public double distance;
		public String docId;
		public Node(double distance, String docId){
			this.distance = distance;
			this.docId = docId;
		}
		
		@Override
		public String toString(){
			return docId;
		}
		
	}
	
	
}
