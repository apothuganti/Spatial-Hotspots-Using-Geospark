package ours;




import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;

public class simple_java {
	public static double roundToTwo(double valu){
		return (double)Math.round(valu*100d)/100d;
	}
	
	
	public static void totalCalculateG(final JavaSparkContext sc, String fileLocation){
		System.out.println("Started");
		SparkConf conf;
	    conf = new SparkConf().setMaster("local").setAppName("MapperProgram");
	    //final JavaSparkContext sc = new JavaSparkContext(conf);
	    final RectangleRDD envRdd = getRectangleRDD(sc);
	    System.out.println("Started created envelopes");
	    List<Envelope> allEnv = getEnvelopeList();
	    System.out.println("Started we got  envelopes");
	    
	    JavaPairRDD <String, Coordinate> day_pointrdd = sc.textFile(fileLocation).mapToPair(
	    		new PairFunction<String, String, Coordinate>() {

			@Override
			public Tuple2<String, Coordinate> call(String t) throws Exception {
				// TODO Auto-generated method stub
				String full_line = t;
				String[] full_line_array = full_line.split(",");
				String date = full_line_array[1];
				double longitude = Double.parseDouble(full_line_array[5]);
				double latitude = Double.parseDouble(full_line_array[6]);
						
				String day = date.split(" ")[0];
				day = day.split("-")[2];
				System.out.println("day"+day);
				GeometryFactory gf = new GeometryFactory();
				Coordinate co = new Coordinate(longitude, latitude);
				System.out.println("long " + longitude + " lat " + latitude);
				return new Tuple2<String, Coordinate>(day, co);
				}
		});
	    
	    JavaPairRDD<String, Iterable<Coordinate>> day_point_iterable_rdd = day_pointrdd.groupByKey();
	    
	    AnotherClass newclass=new AnotherClass(sc, envRdd);
	    JavaPairRDD<String, JavaPairRDD<Envelope, HashSet<Point>>> day_envelopeRDD = 
	    		day_point_iterable_rdd.mapToPair(newclass.function);  
	    Unwrapping unwrappingClass = new Unwrapping(sc, envRdd);
	    JavaPairRDD<String, HashMap<Envelope, List<Long>>> hash_day_env_listRdd = 
	    		day_envelopeRDD.mapToPair(unwrappingClass.unwarpping);
	    
	    Map<String, HashMap<Envelope, List<Long>>> hash_day_env_list = hash_day_env_listRdd.collectAsMap();
	    
	   Map result=new HashMap();
	    double n=1000;//change this
		 // hash_day_env_list=new HashMap();
		double plainSum,squaredSum,denominator,xbar;
		plainSum=squaredSum=denominator=xbar=0;
		for(int i=1;i<31;i++){
			Map mp=(Map) hash_day_env_list.get(""+i);
			java.util.Iterator it =   mp.entrySet().iterator();
		   while (it.hasNext()) {
		       Map.Entry pair =   (Entry) it.next();
		     ArrayList listO=(ArrayList) pair.getValue();
		     
		     for(int j=0;j<listO.size();j++){
		   	 
		   	 plainSum=plainSum+(double)listO.get(i);
		   	 squaredSum=squaredSum+Math.pow((double) listO.get(i), 2);
		   	 
		     }
		      	xbar=plainSum/n;
		      	double denomsqterm= Math.sqrt((n*listO.size() -Math.pow(listO.size(), 2))/(n-1)),S=Math.sqrt((squaredSum/n)-Math.pow(xbar,2));
		      denominator= denomsqterm*S;
		      double gi=(plainSum-xbar*listO.size())/denominator;
		      result.put(i+"_"+pair.getKey().toString(), gi);
		      System.out.println(i+"_"+pair.getKey().toString()+" GI* value is "+gi);
	    
	    
		   }
		}
	    
	    
	    
	    
	    
	    
	    
	    //JavaPa
	    
	    
	    //<day, Hash<envelope, count>> = 
	  /*  PointRDD yesterdaypt = null;
		PointRDD todaypt = getPointRDDDay(sc, 1);
		PointRDD tomorrowpt;*/
		//Here string key = day_lat_long
		//HashMap<String,List<Long>> mapperHash = new HashMap<String, List<Long>>();
		/*for (int i = 1; i <= 31; i++) {
			if (i < 31){
				tomorrowpt = getPointRDDDay(sc,i+1);
			} else {
				tomorrowpt = null;
			}
			
			//To-do remainder yestJavardd for the firstday corner case
			JoinQuery yestjoinqry = new JoinQuery(sc, yesterdaypt, envRdd);
			//JoinQuery yestjoinqry = new JoinQuery(sc, todaypt, envRdd);
			JavaPairRDD<Envelope, HashSet<Point>> yestJavaRDD = yestjoinqry.SpatialJoinQuery(yesterdaypt, envRdd);
			//	JavaPairRDD<Envelope, HashSet<Point>> yestJavaRDD = yestjoinqry.SpatialJoinQuery(todaypt, envRdd);
				
			JoinQuery todayjoinqry = new JoinQuery(sc, todaypt, envRdd);
			JavaPairRDD<Envelope, HashSet<Point>> todayJavaRDD = todayjoinqry.SpatialJoinQuery(todaypt, envRdd);
			
			JoinQuery tomjoinqry = new JoinQuery(sc, tomorrowpt, envRdd);
			JavaPairRDD<Envelope, HashSet<Point>> tomorrowJavaRDD = tomjoinqry.SpatialJoinQuery(tomorrowpt, envRdd);
			
			Map<Envelope, Long> yestEnvCount = yestJavaRDD.countByKey();
			Map<Envelope, Long> todayEnvCount = yestJavaRDD.countByKey();
			Map<Envelope, Long> tomEnvCount = yestJavaRDD.countByKey();
			System.out.println("Started created  count maps");
			//List<Envelope> allEnv = envRdd.collect();
			
			//Here is the main logic
			for(int each_env = 0; each_env < allEnv.size(); each_env+=1){
				Envelope curr_env = allEnv.get(each_env);
				//construct key
				String key = i + "_"+ curr_env.getMinX() + "_" + curr_env.getMinY();
				List<Envelope> neighbours= createNeighbours(curr_env);
				List<Long> values = new ArrayList<Long>();
				//You get 9 neighbours for the each_env;

				for(int l = 0; l < neighbours.size(); l++){
					long yesterdayPts = yestEnvCount.get(neighbours.get(l));
					long todayPts = todayEnvCount.get(neighbours.get(l));
					long tomPts = tomEnvCount.get(neighbours.get(l));
					
					values.add(yesterdayPts);
					values.add(todayPts);
					values.add(tomPts);
				}
				mapperHash.put(key, values);
			}
			
			yesterdaypt = todaypt;
			todaypt = tomorrowpt;
		}*/
		sc.close();
	}
	
	
	
	public static List<Envelope> createNeighbours(Envelope env){
		List<Envelope> envNeighbours = new ArrayList<Envelope>();
		double minX = env.getMinX();
		double minY = env.getMinY();
		double maxX = env.getMaxX();
		double maxY = env.getMaxY();
		
		for (double x = roundToTwo(minX - 0.01); x <= roundToTwo(maxX + 0.01); x = roundToTwo(x+0.01)){
			for (double y = roundToTwo(minY - 0.01); y <= roundToTwo(maxY + 0.01); y =roundToTwo(y+0.01)){
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
			}
			i=roundToTwo(i+0.01);
		}
		return envList;
		
		
	}
	
	
	public static RectangleRDD getRectangleRDD(JavaSparkContext sc){
		double min_lat = 40.5;
		double max_lat = 40.9;
		double min_long = -74.25;
		double max_long = -73.7;
		ArrayList<Envelope> envList = new ArrayList<Envelope>(); 
		
		for(double i=min_lat; i <= max_lat; i = i+0.01){
			for(double j = max_long; j>min_long; j = j-0.01){
				Envelope envelope = new Envelope(i,i+0.01,j,j-0.01);
				envList.add(envelope);
			}
		}
		JavaRDD<Envelope> jrdd;
		jrdd = sc.parallelize(envList);
		RectangleRDD envRdd = new RectangleRDD(jrdd);
		return envRdd;
		
		
		
	}
	
	/*public static PointRDD getPointRDDDay(JavaSparkContext sc,int curDay) {
		
		CSVReader reader;
		PointRDD ptRdd;
		List<Point> tempList = new ArrayList<Point>();
			
		try {
			reader = new CSVReader(new FileReader("/home/vm1/Documents/sample10k.csv"));
				//reader = new CSVReader(new FileReader("hdfs://vm1/revu/sample10k.csv"));
			
			String[] nextLine;
			
			while ((nextLine = reader.readNext()) != null) {
				String date_str = nextLine[0];
				date_str = date_str.split(" ")[0];
				int day = Integer.parseInt(date_str.split("-")[2]);
				if (curDay == day){
					System.out.println("day"+day);
					double longitude = Double.parseDouble(nextLine[1]); 
					double lat = Double.parseDouble(nextLine[2]);
					GeometryFactory gf = new GeometryFactory();
					Coordinate co = new Coordinate(longitude, lat);
					Point pt = gf.createPoint(co);
					tempList.add(pt);
					System.out.println("long " + longitude + " lat " + lat);
				}
			}
			reader.close();
			}
			catch(Exception e){
				
			}
			JavaRDD<Point> jrdd;
			jrdd = sc.parallelize(tempList);
			ptRdd = new PointRDD(jrdd);
		
		return ptRdd;
				
		//return ptRdd;
	}*/
}

class AnotherClass{
	static JavaSparkContext sc;
	static RectangleRDD envRdd;
	AnotherClass(JavaSparkContext sc,RectangleRDD envRdd){
		this.sc=sc;
		this.envRdd=envRdd;
	}
	static PairFunction<Tuple2<String,Iterable<Coordinate>>, String, JavaPairRDD<Envelope, HashSet<Point>>> function=new PairFunction<Tuple2<String,Iterable<Coordinate>>, String, JavaPairRDD<Envelope, HashSet<Point>>>() {

		@Override
		public Tuple2<String, JavaPairRDD<Envelope, HashSet<Point>>> call(
				Tuple2<String, Iterable<Coordinate>> t)
				throws Exception {
			List<Point> tempList = new ArrayList<Point>();
			GeometryFactory gf = new GeometryFactory();
			for (Coordinate en : t._2()) {
				Coordinate co = new Coordinate(en);
				Point pt = gf.createPoint(co);
				tempList.add(pt);
			}
			
			JavaRDD<Point> jrdd;
			jrdd = sc.parallelize(tempList);
			PointRDD ptRdd = new PointRDD(jrdd);
			
			JoinQuery joinqry = new JoinQuery(sc, ptRdd, envRdd);
			JavaPairRDD<Envelope, HashSet<Point>> point_envelope_JavaRDD = joinqry.SpatialJoinQuery(ptRdd, envRdd);
			
						
			return new Tuple2<String, JavaPairRDD<Envelope,HashSet<Point>>>(t._1(), point_envelope_JavaRDD);
		}

		
	};
}


class Unwrapping {
	static JavaSparkContext sc;
	static RectangleRDD envRdd;
	Unwrapping(JavaSparkContext sc,RectangleRDD envRdd){
		this.sc=sc;
		this.envRdd=envRdd;
	}
	static PairFunction<Tuple2<String,JavaPairRDD<Envelope,HashSet<Point>>>, String, HashMap<Envelope, List<Long>>> unwarpping= new PairFunction<Tuple2<String,JavaPairRDD<Envelope,HashSet<Point>>>, String, HashMap<Envelope, List<Long>>>() {

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
				//construct key
				//String key = i + "_"+ curr_env.getMinX() + "_" + curr_env.getMinY();
				List<Envelope> neighbours= createNeighbours(curr_env);
				//valueList.add(env_size);
				long value_sum = env_size;
				long neighbourSize = 1;
				for(int each_env = 0; each_env < neighbours.size(); each_env+=1){
					//long each_count = (long)map.get(neighbours.get(each_env)).size();
					if (map.containsKey(neighbours.get(each_env))){
						//valueList.addAll((List)map.get(neighbours.get(each_env)));
						valueList.add( (long)map.get(neighbours.get(each_env)).size());
						neighbourSize++;
					}
					
				}
				//valueList.add(value_sum);
				/*valueList.add(neighbourSize);
				*/env_values_map.put(curr_env, valueList);
			}
			
			return new Tuple2<String, HashMap<Envelope, List<Long>>>(t._1(), env_values_map);
			// TODO Auto-generated method stub
		}
	
	};

	public static List<Envelope> createNeighbours(Envelope env){
		List<Envelope> envNeighbours = new ArrayList<Envelope>();
		double minX = env.getMinX();
		double minY = env.getMinY();
		double maxX = env.getMaxX();
		double maxY = env.getMaxY();
		
		for (double x = minX - 0.01; x <= maxX + 0.01; x += 0.01){
			for (double y = minY - 0.01; y <= maxY + 0.01; y += 0.01){
				Envelope curEnv = new Envelope(x, x+0.01, y, y+0.01);
				envNeighbours.add(curEnv);
			}
		}		
		return envNeighbours;
	}
	

}
	