package org.datasyslab.geospark.mainclass;




import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class simple_java {
	public static double roundToTwo(double valu){
		return (double)Math.round(valu*100d)/100d;
	}
	public static int counter=0;
	public static double totalNoOfEnv;
	public static class ValueComparator implements Comparator<String>{
		 
		HashMap<String, Double> map = new HashMap<String, Double>();
	 
		public ValueComparator(HashMap<String, Double> map){
			this.map.putAll(map);
		}
	 
		@Override
		public int compare(String s1, String s2) {
			if(map.get(s1) >= map.get(s2)){
				return -1;
			}else{
				return 1;
			}	
		}
	}
	public static TreeMap<String, Double> sortMapByValue(HashMap<String, Double> map){
		Comparator<String> comparator = new ValueComparator(map);
		//TreeMap is a map sorted by its keys. 
		//The comparator is used to sort the TreeMap by keys. 
		TreeMap<String, Double> result = new TreeMap<String, Double>(comparator);
		result.putAll(map);
		return result;
	}
	
	public static void totalCalculateG(final JavaSparkContext sc, String fileLocation){
		System.out.println("Started");
		//SparkConf conf;
	   // conf = new SparkConf().setMaster("local").setAppName("MapperProgram");
	    
	    final RectangleRDD envRdd = getRectangleRDD(sc);
	    System.out.println("Started created envelopes");
	   // List<Envelope> allEnv = getEnvelopeList();
	    System.out.println("Started we got  envelopes");
	    
	    
	    JavaPairRDD <String, Coordinate> day_pointrdd = sc.textFile(fileLocation).mapToPair(
	    		new PairFunction<String, String, Coordinate>() {

			@Override
			public Tuple2<String, Coordinate> call(String t) throws Exception {
				// TODO Auto-generated method stub
				if(counter==0){counter++;return null;}
				String full_line = t;
				String[] full_line_array = full_line.split(",");
				String date = full_line_array[1];
				double longitude = Double.parseDouble(full_line_array[5]);
				double latitude = Double.parseDouble(full_line_array[6]);
						
				String day = date.split(" ")[0];
				day = day.split("-")[2];
				
				GeometryFactory gf = new GeometryFactory();
				Coordinate co = new Coordinate(longitude, latitude);
			
				return new Tuple2<String, Coordinate>(day, co);
				}
		});
	 
	    JavaPairRDD<String, Iterable<Coordinate>> day_point_iterable_rdd = day_pointrdd.groupByKey();

		 
	    AnotherClass newclass=new AnotherClass(sc, envRdd);
	    JavaPairRDD<String, PointRDD> day_envelopeRDD = 
	    		day_point_iterable_rdd.mapToPair(newclass.function);
	    
	    Joiner joiner=new Joiner(sc, envRdd);
	    JavaPairRDD<String,JavaPairRDD<Envelope,HashSet<Point>>> day_envelopes = day_envelopeRDD.mapToPair(joiner.joining);

		   System.out.println("point 3");
	    JavaPairRDD<String, HashMap<Envelope, List<Long>>> hash_day_env_listRdd = 
	    		day_envelopes.mapToPair(new PairFunction<Tuple2<String,JavaPairRDD<Envelope,HashSet<Point>>>, String, HashMap<Envelope,List<Long>>>() {

					@Override
					public Tuple2<String, HashMap<Envelope, List<Long>>> call(
							Tuple2<String, JavaPairRDD<Envelope, HashSet<Point>>> t)
							throws Exception {
						Map<Envelope,HashSet<Point>> map=t._2().collectAsMap();
						
						HashMap<Envelope, List<Long>> env_values_map = new HashMap<>();
						List<Long> valueList = new ArrayList<>();
						for(Envelope env:map.keySet()) {
							valueList = new ArrayList<>();
							long env_size = (long)map.get(env).size();
							Envelope curr_env = env;
							List<Envelope> neighbours= createNeighbours(curr_env);
							 
							long value_sum = env_size;
							long neighbourSize = 1;
							for(int each_env = 0; each_env < neighbours.size(); each_env+=1){
								 if (map.containsKey(neighbours.get(each_env))){ 
									valueList.add( (long)map.get(neighbours.get(each_env)).size());
									neighbourSize++;
								}
												}
							 	env_values_map.put(curr_env, valueList);
							
						}
						
						return new Tuple2<String, HashMap<Envelope, List<Long>>>(t._1(), env_values_map);
					}

					
				});

		  
		   Map<String, HashMap<Envelope, List<Long>>> hash_day_env_list = null;
		   try{
	    hash_day_env_list = hash_day_env_listRdd.collectAsMap();
		   }catch(Exception e){
			  System.out.println(e.getMessage());
		   }
		   
		   
	   HashMap result=new HashMap();
	     //change this
		 // hash_day_env_list=new HashMap();
		long plainSum,squaredSum;
		double denominator,xbar;
		plainSum=squaredSum=0;
				denominator=xbar=0.0;int count=0;
				List<String> stro=new ArrayList<String>();
			List<Double> val=new ArrayList<Double>();
			List<Double> str=new ArrayList<Double>();
			for(String it:hash_day_env_list.keySet()){
			//	System.out.println("here");
				for(Envelope iterator:hash_day_env_list.get(it).keySet()){
					count=0;plainSum=squaredSum=0;
					ArrayList<Long> listToday=(ArrayList) hash_day_env_list.get(it).get(iterator);
			//		System.out.println("here the size ba :"+listO.size());
					count=count+listToday.size();
				
				//previous day	
					int temp=Integer.parseInt(it);
				temp=temp-1;
					if(hash_day_env_list.containsKey(""+temp)){
						
						ArrayList<Long> listPrevios=(ArrayList) hash_day_env_list.get(""+temp).get(iterator);
					 	
						count=count+listPrevios.size();
						

					     for(int j=0;j<listPrevios.size();j++){
					    	 
					   	 plainSum=plainSum+(long)listPrevios.get(j);
					  
					   	 squaredSum=squaredSum+((long)listPrevios.get(j) * (long)listPrevios.get(j));
					      }
						
					}
					
					//next day
					
					  temp=Integer.parseInt(it);
					temp=temp+1;
						if(hash_day_env_list.containsKey(""+temp)){
							
							ArrayList<Long> listNext=(ArrayList) hash_day_env_list.get(""+temp).get(iterator);
							 
							count=count+listNext.size();
						//System.out.println("here the size ba of next day  :"+listN.size());

						     for(int j=0;j<listNext.size();j++){
						    	 
						    	 
						    	 
						   	 plainSum=plainSum+(long)listNext.get(j);
						  
						   	 squaredSum=squaredSum+((long)listNext.get(j) * (long)listNext.get(j));
						      }
							
						}
					
					
					
					
					
					
					
					//today
					
				     for(int j=0;j<listToday.size();j++){
				    	 
				    	 
				    	 
				   	 plainSum=plainSum+(long)listToday.get(j);
				  
				   	 squaredSum=squaredSum+((long)listToday.get(j) * (long)listToday.get(j));
				      }
				      	xbar=plainSum/totalNoOfEnv;
				      	double denomsqterm= Math.sqrt((totalNoOfEnv*count -Math.pow(count, 2))/(totalNoOfEnv-1)),S=Math.sqrt((squaredSum/totalNoOfEnv)-Math.pow(xbar,2));
				       denominator= denomsqterm*S;
				       double gi=(plainSum-xbar*count)/denominator;
				       if(!Double.isNaN(gi)){
				  
				      result.put(it+"_"+iterator.toString(), gi);
				         
				      val.add(gi);
				      stro.add(it+"_"+iterator.toString());
				      }
				      
			    
				}
			} 
			double max=-100;int index=0;
			List x=new ArrayList();
			for(int j=0;j<50;j++){
				max=-100;  index=0;
			for(int i=0;i<val.size();i++){
				if(val.get(i)>max){
					max=val.get(i);
					index=i;
				}
			}
			x.add(val.get(index)+" <-gi    env->      "+stro.get(index));
			System.out.println(val.get(index)+" <-gi    env->      "+stro.get(index));
			val.remove(index);
			}
			System.out.println("finallyu printing n once"+totalNoOfEnv);
	    
			 
	 
		sc.close();
	}
	
	
	
	public static List<Envelope> createNeighbours(Envelope env){
		List<Envelope> envNeighbours = new ArrayList<Envelope>();
		double minX = env.getMinX();
		double minY = env.getMinY();
		double maxX = env.getMaxX();
		double maxY = env.getMaxY();
		
		for (double x = roundToTwo(minX - 0.01); x <= roundToTwo(minX + 0.01); x = roundToTwo(x+0.01)){
			for (double y = roundToTwo(minY - 0.01); y <= roundToTwo(minY + 0.01); y =roundToTwo(y+0.01)){
				Envelope curEnv = new Envelope(x, roundToTwo(x+0.01), y, roundToTwo(y+0.01));
				envNeighbours.add(curEnv);
			}
		}		
		return envNeighbours;
	}
	

	public static List<Envelope> getEnvelopeList(){
		double min_lat = 40.5;
		double max_lat = 40.9;
		double min_long = -74.25;
		double max_long = -73.7;
		ArrayList<Envelope> envList = new ArrayList<Envelope>(); 
		
		for(double i=min_lat; i <= max_lat;){
			for(double j = max_long; j>min_long;){
				Envelope envelope = new Envelope(i,roundToTwo(i+0.01),j,roundToTwo(j-0.01));
				envList.add(envelope);
				j=roundToTwo(j-0.01);
				totalNoOfEnv=totalNoOfEnv+1;
			}
			i=roundToTwo(i+0.01);
		}
		return envList;
		
		
	}
	
	
	public static RectangleRDD getRectangleRDD(JavaSparkContext sc){
		int count=0;List<Envelope> grid =new ArrayList<Envelope>();
		for(double i = -74.25; i<-73.7;){
			for(double j = 40.5; j<40.9;){
				double minX = i;
				double minY = j;
				double maxX = roundToTwo(i+0.01);
				double maxY = roundToTwo(j+0.01);
				grid.add(new Envelope(roundToTwo(minX), roundToTwo(maxX), roundToTwo(minY), roundToTwo(maxY)));
				j=roundToTwo(j+0.01);
				count++;
			}
			i=roundToTwo(i+0.01);
		}
		//n=count;
		 
		JavaRDD<Envelope> javaRDD = sc.parallelize(grid);
		RectangleRDD rectangleRDD = new RectangleRDD(javaRDD);
		totalNoOfEnv=count;
		return rectangleRDD;
		
		
	}
	
	 }

class AnotherClass{
	static JavaSparkContext sc;
	static RectangleRDD envRdd;
	AnotherClass(JavaSparkContext sc,RectangleRDD envRdd){
		this.sc=sc;
		this.envRdd=envRdd;
	}
	static PairFunction<Tuple2<String,Iterable<Coordinate>>, String, PointRDD> function=new PairFunction<Tuple2<String,Iterable<Coordinate>>, String, PointRDD>() {

		@Override
		public Tuple2<String, PointRDD> call(
				Tuple2<String, Iterable<Coordinate>> t)
				throws Exception {
			List<Point> tempList = new ArrayList<Point>();
			GeometryFactory gf = new GeometryFactory();
			for (Coordinate en : t._2()) {
				Coordinate co = en;
				Point pt = gf.createPoint(co);
				tempList.add(pt);
			}
			//System.out.println("mytemp list size:"+tempList.size());
			PointRDD ptRdd = new PointRDD(sc.parallelize(tempList),"rtree",3);
			
		 
			return new Tuple2<String, PointRDD>(t._1(), ptRdd);
		}

		
	};
}
class Joiner{
	static JavaSparkContext sc;
	static RectangleRDD envRdd;
	Joiner(JavaSparkContext sc,RectangleRDD envRdd){
		this.sc=sc;
		this.envRdd=envRdd;
	}
	static PairFunction<Tuple2<String, PointRDD>, String, JavaPairRDD<Envelope, HashSet<Point>>> joining=new PairFunction<Tuple2<String,PointRDD>, String, JavaPairRDD<Envelope,HashSet<Point>>>() {

		@Override
		public Tuple2<String, JavaPairRDD<Envelope, HashSet<Point>>> call(
				Tuple2<String, PointRDD> t) throws Exception {
			JoinQuery joinqry = new JoinQuery(sc, t._2(), envRdd);
			JavaPairRDD<Envelope, HashSet<Point>> point_envelope_JavaRDD = joinqry.SpatialJoinQuery(t._2(), envRdd);
			
			Map<Envelope,HashSet<Point>> mapTemp=point_envelope_JavaRDD.collectAsMap();
		 
			
			return new Tuple2(t._1(),point_envelope_JavaRDD);
		}
	}; 
}



	