package com.asu.dds.group36_phase3_source;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Group36 {
	
	    static JavaSparkContext sc = new JavaSparkContext();

	    private static Function<Tuple2<String, Integer>, Boolean> removenoise = new Function<Tuple2<String, Integer>, Boolean>() {

	        public Boolean call(Tuple2<String, Integer> data) throws Exception {
	            if (data == null) {
	                return false;
	            }
	            return data._2 != 1;
	        }
	    };

	    public static void main(String[] args) throws Exception {
	        double sint = 0.0f;
	        double xbar = 0.0f;
	        double s = 0.0f;
	        if (args.length < 2) {
	            System.err.println("Usage: Group36 <InputFilePath> <OutputDirectoryPath>");
	            System.exit(1);
	        }

	        SparkSession spark = SparkSession
	                .builder()
	                .appName("Group36")
	                .getOrCreate();

	        HashMap<String, Integer> scores = new HashMap<String, Integer>();
	        int day = 0;
	        int lat = 4050;
	        int lon = -7425;
	        for (int i = 0; i < 31; i++) {
	            for (int j = 1; j < 41; j++) {
	                for (int k = 1; k < 56; k++) {
	                    String temp = (day + i) + "," + (lat + j) + "," + (lon + k);
	                    scores.put(temp, 0);
	                }
	            }
	        }
	        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

	        JavaPairRDD<String, Integer> ones = lines.mapToPair(
	                new PairFunction<String, String, Integer>() {
	                    @Override
	                    public Tuple2<String, Integer> call(String s) {

	                        try {
	                            String[] columns = s.split(",");
	                            Float lat = Float.parseFloat(columns[6]);
	                            Float lon = Float.parseFloat(columns[5]);
	                            if ((40.50 <= lat && lat <= 40.90) && (lon <= (-73.70) && (-74.25) <= lon)) {
	                                double latitude = Math.floor(lat * 100);
	                                double longitude = Math.floor(lon * 100);
	                                
	                                String[] date = columns[1].split(" ");
	                                String abc[] = date[0].split("-");
	                                int time_step = Integer.parseInt(abc[2])-1;
	                                String key = (time_step + "," + String.valueOf((int) latitude) + "," + String.valueOf((int) longitude));
	                                return new Tuple2<String, Integer>(key, 0);
	                            } else {
	                                return new Tuple2<String, Integer>("abc", 1);
	                            }
	                        } catch (Exception e) {
	                            System.out.println("Something went wrong while parsing input: " + e.getMessage());
	                            return null;
	                        }
	                    }
	                });
	        JavaPairRDD<String, Integer> onetemp = ones.filter(removenoise);
	        JavaPairRDD<String, Iterable<Integer>> oneDayData = onetemp.groupByKey();
	        List<Tuple2<String, Iterable<Integer>>> output = oneDayData.collect();


	        for (Tuple2<?, ?> tuple : output) {
	            if (!((String) tuple._1()).equals("abc")) {
	                @SuppressWarnings("unchecked")
					Iterable<Integer> iterablePts = (Iterable<Integer>) tuple._2();
	                Iterator itr = iterablePts.iterator();
	                int c = 0;
	                while(itr.hasNext()){
	                	c++;
	                	itr.next();
	                }
	                
	                if (scores.containsKey((String) tuple._1())) {
	                    scores.put((String) tuple._1(), c);
	                }
	            }

	        }

	        Collection c = scores.values();
	        Iterator itr = c.iterator();
	        while (itr.hasNext()){
	        	Integer attr = (Integer)itr.next();
	            xbar += attr.doubleValue();
	            sint += attr.doubleValue() * attr.doubleValue();
	        }
	        System.out.println("Xbar: "+ xbar);
	        System.out.println("Sint: "+ sint);
	        xbar = xbar / 68200;
	        s = Math.sqrt(sint / 68200 - (xbar*xbar));
	        System.out.println("mean = " + xbar);
	        System.out.println("var = " + s);


	        ArrayList<template> result = new ArrayList<template>();
	        int surround[] = {-1, 0, 1};
	        for (Map.Entry<String, Integer> iterval : scores.entrySet()) {
	            String key[] = iterval.getKey().split(",");
	            double xj = 0.0;
	            int date = Integer.parseInt(key[0]);
	            int lats = Integer.parseInt(key[1]);
	            int longs = Integer.parseInt(key[2]);
	            int n = 0;
	            for (int i : surround) {
	                for (int j : surround) {
	                    for (int k : surround) {

	                        String temp = (date + i) + "," + (lats + j) + "," + (longs + k);
	                        if (scores.containsKey(temp)) {
	                            xj += scores.get(temp);
	                            n++;
	                        }
	                    }
	                }
	            }

	            double r = (xj - n * xbar)/(s * Math.sqrt((68200*n - Math.pow(n, 2))/(68200 - 1)));
	            template obj = new template();
	            obj.values = iterval.getKey();
	            obj.zscore = r;
	            result.add(obj);
	        }
	        Collections.sort(result, new comp());
	        List<template> finallist = result.subList(0, 50);

	        for (template t : finallist) {
	            System.out.println("t.key = " + t.values + " f.zscore " + t.zscore);
	        }
	        
	        File file =new File(args[1]+"/group36_phase3_result.csv");
	    	if (file.exists()){
	    		file.delete();
	    	}  
	    	file.createNewFile();
	    	FileWriter fw = new FileWriter(file,true);
	    	BufferedWriter bw = new BufferedWriter(fw);
	    	PrintWriter pw = new PrintWriter(bw);
	    	
	    	for (template t : finallist) {
	    		String[] field = t.values.split(",");
	    		String entry = field[1] + "," + field[2] + "," + field[0] + "," + t.zscore;
	    		pw.println(entry);
	        }
	    	pw.close();
	        
	        spark.stop();
	    }

	    static class template {
	        public String values;
	        public Double zscore;
	    }

	    static class comp implements Comparator<template> {
	        public int compare(template o1, template o2) {
	            return o2.zscore.compareTo(o1.zscore);
	        }
	    }
}
