package org;

import java.util.ArrayList;

public class Point {
	double x;
	double y;
	
	public Point(double x, double y){
		this.x = x;
		this.y = y;
	}
	
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
	
	double getDistance(Point p1, Point p2){
		return Math.sqrt(Math.pow(p1.x - p2.x, 2) - Math.pow(p1.y - p2.y, 2));
	}
	
	
}
