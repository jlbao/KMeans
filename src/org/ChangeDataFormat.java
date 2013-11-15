package org;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

public class ChangeDataFormat {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		//pullDataFromServer();
		//generateInitCentroids();
		System.out.println(getDimensionCount());
    }
	
	public static int getDimensionCount() throws Exception{
		File pointFile = new File("points");
		BufferedReader fr = new BufferedReader(new FileReader(pointFile));
		
		String inputLine;
		int dimensionCount = 0;
		while((inputLine = fr.readLine()) != null){
			dimensionCount = inputLine.split(",").length;
			break;
		}
		fr.close();
		return dimensionCount;
	}
	
	
	public static void generateInitCentroids() throws Exception{
		File pointFile = new File("points");
		BufferedReader fr = new BufferedReader(new FileReader(pointFile));
		
		
        File initCentroidfile = new File("initCentroids");
		FileWriter fw = new FileWriter(initCentroidfile.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		String inputLine;
		int count = 0;
		while((inputLine = fr.readLine()) != null && count < 20){
			bw.write(count + ":" + inputLine + "\n");
			count++;
		}
		fr.close();
		bw.close();
	}
	
	
	public static void pullDataFromServer() throws Exception{
		long startTime = System.currentTimeMillis();
		URL url = new URL("http://64.56.67.181/vectors.txt");
        URLConnection conn = url.openConnection();
        BufferedReader in = new BufferedReader(
                                new InputStreamReader(
                                conn.getInputStream()));
        
        File file = new File("points");
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
        
        String inputLine;
        int count = 0;
        while ((inputLine = in.readLine()) != null){
        	String[] data = inputLine.split("\t");
            bw.write(data[1] + "\n");
            count++;
        }
        in.close();
        bw.close();
        System.out.println(count + "lines are ok");
        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) + " milliseconds");
	}
	

}
