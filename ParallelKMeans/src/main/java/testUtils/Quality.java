package testUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.clustering.ClusteringUtils;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.Centroid;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.stats.*;

import de.tuberlin.dima.ml.inputreader.LibSvmVectorReader;

/*
 *  Currently takes LibSVM format as input

and output as a file containing address of files with final centres
1 0.249170375,0.775122375
2 0.4132631944444444,0.2612536388888889
 */

public class Quality {

	public List<Vector> points = new ArrayList<Vector>();
	public List<Vector> centers = new ArrayList<Vector>();

	public static Vector textToVector(String str)
	{
		
		Vector randomDenseVector = new RandomAccessSparseVector(5);
	     LibSvmVectorReader.readVectorSingleLabel(randomDenseVector, str);
		
		return randomDenseVector;
	}
	
	public static Centroid textToCentroid(int key, String str)
	{
		String[] x=str.split(" ");
		String[] strArray = x[1].split(",");
		double[] dArray = new double[strArray.length];
		for(int i=0; i<strArray.length; i++)
		{
			dArray[i] = Double.parseDouble(strArray[i]);
		}
		Vector v = new DenseVector(dArray);
		Centroid c = new Centroid(key,v);
		return c;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: Quality <in> <centersFileListPath> <out>");
			System.exit(2);
		}
		FileInputStream fis = new FileInputStream(args[0]);
        Scanner scanner = new Scanner(fis);
        FileOutputStream f = new FileOutputStream(args[2]);
        PrintStream ps = new PrintStream(f);
      
        //reading file line by line using Scanner in Java
        System.out.println("Reading file line by line in Java using Scanner");
      
        Quality qualityClass = new Quality();
        while(scanner.hasNextLine()){
        	Vector v = qualityClass.textToVector(scanner.nextLine());
        	qualityClass.points.add(v);
        }
        
        List<List<Vector>> centersList = readCenters(args[1]);
        List<Double> dunnIndexList = new ArrayList<Double>();
        List<Double> daviesBouldinIndexList = new ArrayList<Double>();
        List<Double> totalCostList = new ArrayList<Double>();
        Iterator<List<Vector>> i = centersList.iterator();
        
        while(i.hasNext()){
        	List<Vector> centers=i.next();
        	List<OnlineSummarizer> summarizerList = ClusteringUtils.summarizeClusterDistances(qualityClass.points, centers, new CosineDistanceMeasure());
        	double dunnIndex = ClusteringUtils.dunnIndex(centers, new CosineDistanceMeasure(), summarizerList);
        	double daviesBouldinIndex = ClusteringUtils.daviesBouldinIndex(centers, new CosineDistanceMeasure(), summarizerList);
        	double totalClusterCost = ClusteringUtils.totalClusterCost(qualityClass.points, centers);
        	dunnIndexList.add(dunnIndex);
        	daviesBouldinIndexList.add(daviesBouldinIndex);
        	totalCostList.add(totalClusterCost);
        }
        scanner.close();
        
        Iterator<Double> ix= dunnIndexList.iterator();
        ps.println("Dunn Index");
     
        
        while(ix.hasNext()){
        	Double duni=ix.next();
        	ps.println(duni);
        	   System.out.println("Dunn Index:"+duni);
        }
        
        ix= daviesBouldinIndexList.iterator();
        ps.println("Davies Index");
        while(ix.hasNext()){
        	Double di = ix.next();
        	ps.println(di);
        	 System.out.println("Davies Index:"+di);
        }
        
        ix= totalCostList.iterator();
        ps.println("Total Cost");
        while(ix.hasNext()){
        	Double tc = ix.next();
        	ps.println(tc);
        	 System.out.println("Total Cost:"+tc);
        }
        
        ps.close();

	}
	
	public static List<List<Vector>> readCenters(String path) throws FileNotFoundException
	{
		FileInputStream fis = new FileInputStream(path);
        Scanner scanner = new Scanner(fis);
        List<List<Vector>> centersListList = new ArrayList<List<Vector>>();
      
        //reading file line by line using Scanner in Java
        System.out.println("Reading file line by line in Java using Scanner");
      
        while(scanner.hasNextLine()){
        	FileInputStream fis2 = new FileInputStream(scanner.nextLine());
            Scanner scanner2 = new Scanner(fis2);
            List<Vector> centers = new ArrayList<Vector>();
            //reading file line by line using Scanner in Java
            System.out.println("Reading file line by line in Java using Scanner");
          int key=0;
            while(scanner2.hasNextLine()){
            	Vector v = textToCentroid(key++, scanner2.nextLine());
            	centers.add(v);
            }
            centersListList.add(centers);
            scanner2.close();
        }
        scanner.close();
		return centersListList;
	}
}
