package org;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Random;

public class CreateDataset {

	/*
	 * Create Point and initCentroid file
	 */
	public static void main(String[] args){
		try {
			Config.makeConfiguration();
			ArrayList<Point> initCentroids = new ArrayList<Point>();
			if(writeInitPoints(initCentroids) && writeInitCentroids(initCentroids))
				System.out.println("done");
			else
				System.out.println("not done");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// write init points information into points file
	public static boolean writeInitPoints(ArrayList<Point> initCentroids) throws Exception{
		File file = new File("data/points");
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		for(int i = 0; i < Config.getPointNum() - 1; i++){
			Point p = getRandomPoint();
			bw.write(p.toString() + "\n");
			if(i < Config.getK())
				initCentroids.add(p);
		}
		
		bw.write(getRandomPoint().toString());
		bw.close();
		return true;
	}
	
	// write init centroids into initCentroids file
	public static boolean writeInitCentroids(ArrayList<Point> initCentroids) throws Exception{
		File file = new File(Config.getInitCentroidPath());
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		for(int i = 0 ; i < initCentroids.size() - 1; i++){
			Point p = initCentroids.get(i);
			bw.write(p.toString() + "\n");
		}
		bw.write(initCentroids.get(initCentroids.size() - 1).toString());
		bw.close();
		return true;
	}
	
	// get random point range from
	public static Point getRandomPoint(){
		Random rand = new Random();
		ArrayList<Double> list = new ArrayList<Double>();
		for(int i = 0; i < Config.getDimensionNum(); i++){
			list.add(rand.nextInt(Config.getPointRange()) + rand.nextDouble());
		}
		return new Point(list);
	}
	
}
