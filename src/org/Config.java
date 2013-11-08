package org;

import java.io.File;

public class Config {
	private static int K = 10;
	private static int PointNum = 1000;
	private static boolean IsStopIterationByCount = true;
	private static int IterationCount = 10;
	private static double stopIterationThreshold = 2.0;
	private static String initCentroidPath = "data/initCentroids";
	private static String newCentroidPath = "data/newCentroids";
	
	public void loadConfiguation() throws Exception{
		File file = new File("config.xml");
		if(!file.exists())
			throw new Exception("config file does not exist!");
		
	}
	
	public static int getK() {
		return K;
	}

	public static int getPointNum() {
		return PointNum;
	}

	public static boolean getIsStopIterationByCount() {
		return IsStopIterationByCount;
	}

	public static int getIterationCount() {
		return IterationCount;
	}

	public static double getStopIterationThreshold() {
		return stopIterationThreshold;
	}

	public static String getInitCentroidPath() {
		return initCentroidPath;
	}

	public static String getNewCentroidPath() {
		return newCentroidPath;
	}

}
