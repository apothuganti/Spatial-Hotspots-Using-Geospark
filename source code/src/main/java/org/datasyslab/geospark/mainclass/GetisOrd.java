/* 

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import scala.Tuple2;

*//**
 * Created by anusha on 11/29/16.
 *//*
public class GetisOrd {

	public static JavaSparkContext sc;

	public static List<Envelope> grid = new ArrayList<>();
	public static PointRDD pointRdd;
	public static long xj=0;
	public static long xjsquare=0;
	public static long n=68200;

	public static double roundToTwo(double value) {
		return (double)Math.round(value * 100d) / 100d;
	}

	public static RectangleRDD createEnvelopes(JavaSparkContext sc){
		int count=0;
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
		System.out.println("n value="+n);
		JavaRDD<Envelope> javaRDD = sc.parallelize(grid);
		RectangleRDD rectangleRDD = new RectangleRDD(javaRDD);
		return rectangleRDD;
	}



	@SuppressWarnings("serial")
	private static JavaPairRDD<String,PointRDD> createPointRDD(final JavaSparkContext sc,String ip){

		System.out.println("Starting to read input file=="+ip);
		JavaRDD<String> file=sc.textFile(ip);
		final String firstLine=file.first();

		JavaRDD<String> textFromFile = file.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				return !s.equalsIgnoreCase(firstLine);
			}
		});

		JavaPairRDD<String,Coordinate> pairrdd=textFromFile.mapToPair(new PairFunction<String,String, Coordinate>() {
			@Override
			public Tuple2<String, Coordinate> call(String t) throws Exception {
				String[] arr=t.split(",");
				Tuple2<String, Coordinate> tup=new Tuple2<String, Coordinate>(arr[1].split(" ")[0], new Coordinate(Double.parseDouble(arr[5]), Double.parseDouble(arr[6])));
				//System.out.println(tup.toString());
				return tup;
			}

		});
		
		System.out.println("Input file read done");
		//n=pairrdd.count();
		
		JavaPairRDD<String, Iterable<Coordinate>> grp=pairrdd.groupByKey();
		System.out.println("After Grouping");
		System.out.println("After var decleration");
		DiffClass d=new DiffClass(sc);

		final JavaPairRDD<String, PointRDD> poin=grp.mapToPair(d.var);
		System.out.println("After grp decleration");
		return poin;

	}


	public static JavaPairRDD<String,JavaPairRDD<Envelope,HashSet<Point>>> somethingnew(JavaPairRDD<String, PointRDD> map,final RectangleRDD rectangleRDD, final JavaSparkContext sc){
		System.out.println("Insdie something new");

		DiffClass2 d=new DiffClass2(sc,rectangleRDD);
		JavaPairRDD<String,JavaPairRDD<Envelope,HashSet<Point>>> s=map.mapToPair(d.var);
		return s;

	}

	public static JavaPairRDD<String,HashMap<Envelope,Long[]>> findingNeighbor(JavaPairRDD<String, JavaPairRDD<Envelope, HashSet<Point>>> map, JavaSparkContext sc){

		DiffClass3 d=new DiffClass3(sc, xj, xjsquare);
		JavaPairRDD<String,HashMap<Envelope,Long[]>> ret=map.mapToPair(d.var);

		return ret;

	}

	public static void mainFunction(JavaSparkContext sc,String ip,String op) {
		System.out.println("Before create envelope");
		RectangleRDD rectangleRDD =createEnvelopes(sc);
		System.out.println("After create envelope");
		JavaPairRDD<String,PointRDD> map=createPointRDD(sc,ip);
		System.out.println("Create point done="+map.count());
		JavaPairRDD<String, JavaPairRDD<Envelope, HashSet<Point>>> hashmap=somethingnew(map, rectangleRDD,sc);
		System.out.println("Something new done="+hashmap.count());
		Map<String,HashMap<Envelope,Long[]>> neighbors=findingNeighbor(hashmap,sc).collectAsMap();
		//        System.out.println("Neighbors done="+neighbors.size());
		//        System.out.println("xj="+xj);
		double Xbar=(double)xj/(double)n;
		//        System.out.println("Xbar="+Xbar);
		double S= Math.sqrt(((double)xjsquare/(double)n)-((double)Xbar*(double)Xbar));
		//        System.out.println("xjsquare="+xjsquare);
		//        System.out.println("S="+S);
		List<Wrapper> list=new ArrayList<Wrapper>();


		for(String date : neighbors.keySet()){
			        	System.out.println("Date is =="+date);
			for(Envelope env : neighbors.get(date).keySet()){
				long count = neighbors.get(date).get(env)[0];
				//        		System.out.println("Count="+count);
				Date d =new Date();
				SimpleDateFormat s=new SimpleDateFormat("yyyy-MM-dd");
				try {
					d=s.parse(date);

				} catch (ParseException e) {
					e.printStackTrace();
				}
				Calendar cal = Calendar.getInstance();
				cal.setTime(d);
				cal.add(Calendar.DAY_OF_YEAR,-1);
				Date oneDayBefore= cal.getTime();
				String before = s.format(oneDayBefore);
				long countBefore = neighbors.get(before)==null?0: neighbors.get(before).get(env)[0];

				cal.setTime(d);
				cal.add(Calendar.DAY_OF_YEAR,1);
				Date oneDayAfter= cal.getTime();
				String after = s.format(oneDayAfter);

				long countAfter = neighbors.get(after)==null?0: neighbors.get(after).get(env)[0];
				//        		System.out.println("Count After="+countAfter);
				long countFinal = count + countBefore + countAfter;
				//        		System.out.println("Count Final="+countFinal);
				long weightFinal = neighbors.get(date).get(env)[1] + (neighbors.get(before)==null?0:neighbors.get(before).get(env)[1]) + (neighbors.get(after)==null?0:neighbors.get(after).get(env)[1]);
				//        		System.out.println("weightFinal="+weightFinal);
				double diff = (double)countFinal - ((double)Xbar * (double)weightFinal);
				//        		System.out.println("diff="+diff);
				double sqroot = Math.sqrt((((double)n * (double)weightFinal) - ((double)weightFinal * (double)weightFinal))/(double)(n-1));
				//        		System.out.println("sqroot="+sqroot);

				double value = (double)diff/((double)S * (double)sqroot);
				System.out.println("S * sqroot="+S*sqroot);
        		System.out.println("Value="+value);
				//System.out.println("Date=="+date+";;;Value=="+env.toString()+","+date+","+value);

				list.add(new Wrapper(env,date,value));
				//        		break;
			}
			//        	break;
		}


		Collections.sort(list,new Comparator<Wrapper>() {

			@Override
			public int compare(Wrapper o1, Wrapper o2) {
				if( o1.val- o2.val>0)return -1 ;
				else if( o1.val- o2.val<0)return 1 ;
				else return 0;

			}
		});
		System.out.println("List size=="+list.size()+"=====should be 68200");
		List<String> finallist=new ArrayList<String>();
		int count=0;
		for(Wrapper listStr:list){
			System.out.println((int)listStr.env.getMinX()*100+","+(int)listStr.env.getMinY()*100+","+listStr.date.split("-")[2]+","+listStr.val);
			finallist.add((int)listStr.env.getMinX()*100+","+(int)listStr.env.getMinY()*100+","+listStr.date.split("-")[2]+","+listStr.val);
			count++;
			if(count==50)break;
		}

		JavaRDD<String> wri=sc.parallelize(finallist);
		wri.saveAsTextFile(op+"/outputVandurichiSparkla.txt");

	}
}
class Wrapper{
	Envelope env;
	String date;
	double val;
	public Wrapper(Envelope env,String date,double val){
		this.env=env;
		this.date=date;
		this.val=val;
	}
}

class DiffClass{
	public static JavaSparkContext sc;
	DiffClass(JavaSparkContext sc){
		this.sc=sc;
	}
	static PairFunction<Tuple2<String,Iterable<Coordinate>>, String, PointRDD> var= new PairFunction<Tuple2<String,Iterable<Coordinate>>, String, PointRDD>() {

		@Override
		public Tuple2<String, PointRDD> call(Tuple2<String, Iterable<Coordinate>> t) throws Exception {

			Iterable<Coordinate> i=t._2();
			ArrayList<Point> pointRDDList= new ArrayList<>();
			GeometryFactory fact = new GeometryFactory();
			for(Coordinate c:i){
				Point queryPoint=fact.createPoint(c);
				pointRDDList.add(queryPoint);
			}
			PointRDD p=new PointRDD(sc.parallelize(pointRDDList),"rtree",3);
			Tuple2<String,PointRDD> tup=new Tuple2<String, PointRDD>(t._1(), p);
			return tup;
		}
	};
}
class DiffClass2{
	public static JavaSparkContext sc;
	public static RectangleRDD rectangleRDD;
	DiffClass2(JavaSparkContext sc,RectangleRDD rectangleRDD){
		this.sc=sc;
		this.rectangleRDD=rectangleRDD;
	}
	static PairFunction<Tuple2<String, PointRDD>, String, JavaPairRDD<Envelope, HashSet<Point>>> var= new PairFunction<Tuple2<String,PointRDD>, String, JavaPairRDD<Envelope,HashSet<Point>>>() {

		@Override
		public Tuple2<String, JavaPairRDD<Envelope, HashSet<Point>>> call(Tuple2<String, PointRDD> t) throws Exception {
			JoinQuery joinQuery = new JoinQuery(sc,t._2(),rectangleRDD);
			JavaPairRDD<Envelope, HashSet<Point>> p= joinQuery.SpatialJoinQuery(t._2(), rectangleRDD);

			Tuple2<String, JavaPairRDD<Envelope, HashSet<Point>>> tup=new Tuple2<String, JavaPairRDD<Envelope,HashSet<Point>>>(t._1(), p);

			return tup;
		}
	};
}



class DiffClass3{
	public static JavaSparkContext sc;
	final static double minX=-74.25;
	final static double maxX=-73.7;
	final static double minY=40.5;
	final static double maxY=40.9;
	static long xj;
	static long xjsquare;
	DiffClass3(JavaSparkContext sc,long xj,long xjsquare){
		this.sc=sc;
		this.xj=xj;
		this.xjsquare=xjsquare;
	}
	public static double roundToTwo(double value) {
		return (double)Math.round(value * 100d) / 100d;
	}
	static PairFunction<Tuple2<String, JavaPairRDD<Envelope, HashSet<Point>>>, String, HashMap<Envelope, Long[]>> var= new PairFunction<Tuple2<String,JavaPairRDD<Envelope,HashSet<Point>>>, String, HashMap<Envelope,Long[]>>() {

		@Override
		public Tuple2<String, HashMap<Envelope, Long[]>> call(Tuple2<String, JavaPairRDD<Envelope, HashSet<Point>>> t) throws Exception {
			String date=t._1();
			HashMap<Envelope,Long[]> m=new HashMap<>();
			//			System.out.println("Map size="+t._2().collectAsMap().size());
			Map<Envelope,HashSet<Point>> map=t._2().collectAsMap();
			for(Envelope env:map.keySet()){
				//    			System.out.println("Current env="+env);
				long xval=map.get(env).size();
				xj=xj+xval;
				xjsquare=xjsquare+(xval*xval);
				double x2=env.getMaxX();
				double x1=env.getMinX();
				double y2=env.getMaxY();
				double y1=env.getMinY();
				Long[] lArray = new Long[2];
				long count=0;
				long neighbourCount = 0;
				for(double i=roundToTwo(x1-0.01);i<=roundToTwo(x1+0.01);){
					for(double j=roundToTwo(y1-0.01);j<=roundToTwo(y1+0.01);){
						if(i >= minX && i <= maxX && j >= minY && j <= maxY ){
							Envelope e=new Envelope(i,roundToTwo(i+0.01),j,roundToTwo(j+0.01));
							if(map.get(e)!= null){
								count+=(map.get(e).size());
								//	System.out.println("Current env="+env+";;Count="+count);
								neighbourCount++;
							}
						}
						j=roundToTwo(j+0.01);
					}

					i=roundToTwo(i+0.01);
				}
				lArray[0] = count;
				lArray[1] = neighbourCount;
				m.put(env, lArray);
			}
			GetisOrd.xj=xj;
			GetisOrd.xjsquare=xjsquare;


			return new Tuple2<String, HashMap<Envelope,Long[]>>(t._1(), m);
		}
	};
}*/