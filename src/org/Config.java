package org;

import java.io.File;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Document;

public class Config {
	private static int k;
	private static int pointNum;
	
	// point value range in all dimensions
	private static int pointRange;
	
	// dimension count
	private static int dimensionNum;
	
	// stop iteration by count or by threshold
	private static boolean isStopIterationByCount;
	
	// stop iteration count
	private static int iterationCount;
	
	// stop iteration threshold
	private static double stopIterationThreshold;
	
	// initCentroidPath in hdfs
	private static String initCentroidPath;
	
	// new CentroidPath in hdfs
	private static String newCentroidPath;

	
	// should ensure config.xml is in the same path with the jar file
	public static void makeConfiguration() throws Exception{
		boolean configSuccess = doConfiguation();
		if(!configSuccess)
			setDefaultConfig();
	}
	
	// load configurations from config.xml
	private static boolean doConfiguation() throws Exception{
		File file = new File("config.xml");
		if(!file.exists())
			return false;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(file);
		XPathFactory xPathfactory = XPathFactory.newInstance();
		XPath xpath = xPathfactory.newXPath();
		
		XPathExpression expr = xpath.compile("/config/k");
		k = Integer.parseInt((String)expr.evaluate(doc, XPathConstants.STRING));
		
		expr = xpath.compile("/config/pointNum");
		pointNum = Integer.parseInt((String)expr.evaluate(doc, XPathConstants.STRING));
		
		expr = xpath.compile("/config/pointRange");
		pointRange = Integer.parseInt((String)expr.evaluate(doc, XPathConstants.STRING));
		
		expr = xpath.compile("/config/isStopIterationByCount");
		isStopIterationByCount = Boolean.parseBoolean((String)expr.evaluate(doc, XPathConstants.STRING));
		
		expr = xpath.compile("/config/isStopIterationByCount");
		isStopIterationByCount = Boolean.parseBoolean((String)expr.evaluate(doc, XPathConstants.STRING));
		
		expr = xpath.compile("/config/iterationCount");
		iterationCount = Integer.parseInt((String)expr.evaluate(doc, XPathConstants.STRING));
		
		expr = xpath.compile("/config/stopIterationThreshold");
		stopIterationThreshold = Double.parseDouble((String)expr.evaluate(doc, XPathConstants.STRING));
		
		expr = xpath.compile("/config/initCentroidPath");
		initCentroidPath = (String)expr.evaluate(doc, XPathConstants.STRING);
		
		expr = xpath.compile("/config/newCentroidPath");
		newCentroidPath = (String)expr.evaluate(doc, XPathConstants.STRING);
		
		expr = xpath.compile("/config/dimensionNum");
		dimensionNum = Integer.parseInt((String)expr.evaluate(doc, XPathConstants.STRING));
		
		return true;
	}
	
	// set default configurations if there is no config.xml
	private static void setDefaultConfig(){
		k = 10;
		pointNum = 1000;
		pointRange = 50000;
		isStopIterationByCount = false;
		iterationCount = 10;
		stopIterationThreshold = 3.0;
		initCentroidPath = "data/initCentroids";
		newCentroidPath = "data/newCentroids";
		dimensionNum = 2; 
	}
	
	
	public static int getK() {
		return k;
	}

	public static int getPointNum() {
		return pointNum;
	}

	public static int getPointRange() {
		return pointRange;
	}
	
	public static boolean getIsStopIterationByCount() {
		return isStopIterationByCount;
	}

	public static int getIterationCount() {
		return iterationCount;
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
	
	public static int getDimensionNum(){
		return dimensionNum;
	}

}
