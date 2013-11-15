package org;

import java.util.ArrayList;

public class Point {
	// list stores the values in all dimensions
	ArrayList<Double> list;
	
	public Point(ArrayList<Double> list){
		this.list = list;
	}
	
	// parse line from lines in hdfs file
	public Point(String str){
		list = new ArrayList<Double>();
		String[] s = str.split(",");
		for(String val : s){
			list.add(Double.parseDouble(val));
		}
	}
	
	// get the nearest centroid
	public Point getNearestCentroid(ArrayList<Point> centroids){
		Point nearestCentroid = centroids.get(0); 
		double minDistance = getDistance(nearestCentroid, this);
		for(Point point : centroids){
			double d1 = getDistance(point, this);
			if(d1 < minDistance){
				nearestCentroid = point;
				minDistance = d1;
			}
		}
		return nearestCentroid;
	}
	
	// to make the point represented as the format of (x,y) in order to transmit from Mapper to reducer
	@Override
	public String toString(){
		String str = "";
		for(int i = 0; i < list.size() - 1; i++)
			str += list.get(i) + ",";
		return str + list.get(list.size() - 1);
	}

	
    // p1 and p2 should have same number of dimensions
    public static double getDistance(Point p1, Point p2){
        double val = 0.0;
        for(int i = 0; i < p1.list.size(); i++){
                val += Math.pow(p1.list.get(i) - p2.list.get(i), 2);
        }
        return Math.sqrt(val);
    }
	
	/* this is the task 3 distance calculation version
	// p1 and p2 should have same number of dimensions
	public static double getDistance(Point p1, Point p2){
		double up = 0;
		double xSum = 0;
		double ySum = 0;
		for(int i = 0; i < p1.list.size(); i++){
			up += p1.list.get(i) * p2.list.get(i);
			xSum += Math.pow(p1.list.get(i), 2);
			ySum += Math.pow(p2.list.get(i), 2);
		}
		double result = up / (Math.sqrt(xSum) * Math.sqrt(ySum));
		return result;
	
	}
	*/
}
