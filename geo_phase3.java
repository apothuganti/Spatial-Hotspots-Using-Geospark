package org.datasyslab.geospark.showcase;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.Collator;
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
import java.util.Set;


import scala.Tuple2;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

public class geo_phase3 {
   
    public static void executefunction(){
        int hdfs = 1;
        System.out.println("statreted and connected");
    }
    //Main function
    public static void mainFunction(JavaSparkContext sc,String ip,String op) {
        //get all the envelopes
        getAllEnvelopes(sc);
        executefunction();
        //create points for each variable
        JavaPairRDD<String,HashMap<Envelope,Long>> maday_point_rdd=createDayPointRDD(sc,ip);
        //for each envelope and day get all the possible neighbors
        HashMap<String,HashMap<Envelope,Long[]>> neighbors=getAllNeighbors(maday_point_rdd,sc);
        //create limits and number of envelopes
        double xLimit=(double)xj/(double)nnumber_of_envelopes;
        double numOfEnvelopes= Math.sqrt(((double)xjsquare/(double)nnumber_of_envelopes)-((double)xLimit*(double)xLimit));
        List<Encompas> list=new ArrayList<Encompas>();

        for(String date : neighbors.keySet()){
            for(Envelope env : neighbors.get(date).keySet()){
                long count = neighbors.get(date).get(env)[0];
                Date curr_date =new Date();
                SimpleDateFormat s=new SimpleDateFormat("yyyy-MM-dd");
                //parse the date format and extract
                try {
                    curr_date=s.parse(date);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
               
                Calendar curr_calendar = Calendar.getInstance();
                curr_calendar.setTime(curr_date);
                curr_calendar.add(Calendar.DAY_OF_YEAR,-1);
                Date yesterday= curr_calendar.getTime();
                String yest_string = s.format(yesterday);
                long count_yest = neighbors.get(yest_string)==null?0: neighbors.get(yest_string).get(env)==null?0:neighbors.get(yest_string).get(env)[0];
                curr_calendar.setTime(curr_date);
                curr_calendar.add(Calendar.DAY_OF_YEAR,1);
                Date oneDayAfter= curr_calendar.getTime();
                String tomorrow = s.format(oneDayAfter);
                long yester_count = neighbors.get(tomorrow)==null?0: neighbors.get(tomorrow).get(env)==null?0:neighbors.get(tomorrow).get(env)[0];
                long today_count = count + count_yest + yester_count;

                long final_weight = neighbors.get(date).get(env)==null?0 : neighbors.get(date).get(env)[1] + (neighbors.get(yest_string)==null?0:neighbors.get(yest_string).get(env)==null?0:neighbors.get(yest_string).get(env)[1]) + (neighbors.get(tomorrow)==null?0:neighbors.get(tomorrow).get(env)==null?0:neighbors.get(tomorrow).get(env)[1]);
                //Calculate the get is formula
                double difference = (double)today_count - ((double)xLimit * (double)final_weight);

                double sqroot = Math.sqrt(((nnumber_of_envelopes * (double)final_weight) - ((double)final_weight * (double)final_weight))/(nnumber_of_envelopes-1));

                double value = (double)difference/((double)numOfEnvelopes * (double)sqroot);
                if(!Double.isNaN(value))
                    list.add(new Encompas(env,date,value));
            }
        }

        int first_fifty=0;
        final Collator final_collector = Collator.getInstance();
        Collections.sort(list,new Comparator<Encompas>() {

            @Override
            public int compare(Encompas o1, Encompas o2) {
                if( o2.this_val- o1.this_val==0)
                    return 0;
                return Double.compare(o2.this_val,o1.this_val);
            }
        });
        List<String> resul_array=new ArrayList<String>();
        for(Encompas listStr:list){
            if(first_fifty==51)
                break;
            BigDecimal firstd1 = BigDecimal.valueOf(listStr.this_envelope.getMinX());
            BigDecimal firstd2 = BigDecimal.valueOf(listStr.this_envelope.getMinY());
            BigDecimal firstd3 = BigDecimal.valueOf(100d);
            BigDecimal firstans1 = firstd1.multiply(firstd3);
            BigDecimal firstans2 = firstd2.multiply(firstd3);
            resul_array.add(firstans2.toString().split("\\.")[0]+","+firstans1.toString().split("\\.")[0]+","+(Integer.parseInt(listStr.this_date.split("-")[2])-1)+","+listStr.this_val);
            first_fifty+=1;
        }
        JavaRDD<String> write_file=sc.parallelize(resul_array);
        write_file.saveAsTextFile(op+"/output.csv");
    }

    public static JavaSparkContext sc;
    public static double doubleHandle(double value) {
        return (double)Math.round(value * 100d) / 100d;
    }
    public static PointRDD pointRdd;
    public static long xj=0;
    public static long xjsquare=0;
   
    public static Set<Envelope> envelope_grid = new HashSet<>();
    public static int nnumber_of_envelopes=68200;  
    //This function creates the pointrdd for each day and puts in the javapair rdd structure
   
    public static JavaPairRDD<String,HashMap<Envelope,Long>> createDayPointRDD(JavaSparkContext sc,String ip){

        JavaRDD<String> input_file=sc.textFile(ip);
        final String firstLine=input_file.first();

        JavaRDD<String> inputtext = input_file.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return !s.equalsIgnoreCase(firstLine);
            }
        });
        JavaPairRDD<String,String[]> co_ordinates=inputtext.mapToPair(new PairFunction<String, String, String[]>() {
            @Override
            public Tuple2<String, String[]> call(String arg0) throws Exception {
                String[] arr=arg0.split(",");
                String day = arr[1].split(" ")[0];
                String[] s1=arr[5].split("\\.");
                String[] s2=arr[6].split("\\.");
                String str1=s1[0];
                String str_variable = s1[0];
                if(s1.length>1){
                    str1+=("."+s1[1].substring(0,Math.min(2, s1[1].length())));
                    double some_string = Double.parseDouble(str1);
                    some_string= doubleHandle(some_string-0.01);
                    str_variable = String.valueOf(some_string);
                }
                String str2=s2[0];
                if(s2.length>1)str2+=("."+s2[1].substring(0,Math.min(2, s2[1].length())));
                String[] strarr=new String[2];

                strarr[0]=str_variable;
                strarr[1]=str2;
                Tuple2<String, String[]> return_tuple=new Tuple2<String, String[]>(day, strarr);
                return return_tuple;
       
            }
        });
       
        long n=co_ordinates.count();
        JavaPairRDD<String, Iterable<String[]>> co_ordinateKey=co_ordinates.groupByKey();
        JavaPairRDD<String,HashMap<Envelope,Long>> envelope_values=co_ordinateKey.mapToPair(new PairFunction<Tuple2<String,Iterable<String[]>>, String, HashMap<Envelope,Long>>() {
            @Override
            public Tuple2<String, HashMap<Envelope, Long>> call(
                    Tuple2<String, Iterable<String[]>> arg0) throws Exception {
                HashMap<Envelope,Long> map=new HashMap<Envelope,Long>();
                for(String[] get_coordinate:arg0._2()) {
                    double lowerx = Double.parseDouble(get_coordinate[0]);
                    double lowery = Double.parseDouble(get_coordinate[1]);
                    Envelope envelope=new Envelope(lowerx,doubleHandle(lowerx+0.01),lowery,(lowery+0.01));
                    if(envelope_grid.contains(envelope)){
                        if(map.containsKey(envelope))map.put(envelope, map.get(envelope)+1);
                        else map.put(envelope, (long)1);
                    }
                }
                return new Tuple2<String, HashMap<Envelope,Long>>(arg0._1(), map);
            }
        });    

        return envelope_values;
    }

    public static void getAllEnvelopesfromrectangle(JavaSparkContext sc){       
            for(double i = -74.25; i<-73.7;){
                for(double j = 40.5; j<40.9;){
                    double minX = i;
                    double minY = j;
                    double maxX = i+0.01;
                    double maxY = j+0.01;
                    envelope_grid.add(new Envelope(doubleHandle(minX), doubleHandle(maxX), doubleHandle(minY), doubleHandle(maxY)));
                    j=doubleHandle(j+0.01);
                }
                i=doubleHandle(i+0.01);
            }
        }
    //Create all possible from the envelopes
    public static void getAllEnvelopes(JavaSparkContext sc){       
            for(double i = -74.25; i<-73.7;){
                for(double j = 40.5; j<40.9;){
                    double minX = i;
                    double minY = j;
                    double maxX = i+0.01;
                    double maxY = j+0.01;
                    envelope_grid.add(new Envelope(doubleHandle(minX), doubleHandle(maxX), doubleHandle(minY), doubleHandle(maxY)));
                    j=doubleHandle(j+0.01);
                }
                i=doubleHandle(i+0.01);
            }
        }

    //for a point get all the neighbors for each envelope and map
   
    public static HashMap<String,HashMap<Envelope,Long[]>> getAllNeighbors(JavaPairRDD<String,HashMap<Envelope,Long>> map, JavaSparkContext sc){
        Map<String, HashMap<Envelope, Long>> hash_collect=map.collectAsMap();
        HashMap<String,HashMap<Envelope,Long[]>> result_map=new HashMap<>();
        double min_x=-74.25;
        double max_x=-73.7;
        double min_y=40.5;
        double max_y=40.9;
        for(String date:hash_collect.keySet()){
            HashMap<Envelope,Long[]> map_value=new HashMap<>();
            for(Envelope env:hash_collect.get(date).keySet()){
                long val_x=hash_collect.get(date).get(env);
                xj=xj+val_x;
                xjsquare=xjsquare+(val_x*val_x);
                BigDecimal big_d = BigDecimal.valueOf(env.getMaxX());
                big_d=big_d.setScale(2, RoundingMode.DOWN);
                double x2=big_d.doubleValue();
                BigDecimal big_d2 = BigDecimal.valueOf(env.getMinX());
                big_d2=big_d2.setScale(2, RoundingMode.DOWN);
                double x1=big_d2.doubleValue();
                BigDecimal big_d3 = BigDecimal.valueOf(env.getMaxY());
                big_d3=big_d3.setScale(2, RoundingMode.DOWN);
                double y2= big_d3.doubleValue();
                BigDecimal big_d4 = BigDecimal.valueOf(env.getMinY());
                big_d4=big_d4.setScale(2, RoundingMode.DOWN);
                double y1=big_d4.doubleValue();
                Long[] listofLongs = new Long[2];
                long count=0;
                long num_neighbours = 0;
                for(double i=doubleHandle(x1-0.01);i<=doubleHandle(x1+0.01);){
                    for(double j=doubleHandle(y1-0.01);j<=doubleHandle(y1+0.01);){
                        if(i >= min_x && i < max_x && j >= min_y && j < max_y ){
                            Envelope eachEnv=new Envelope(i,doubleHandle(i+0.01),j,doubleHandle(j+0.01));
                            if(hash_collect.get(date).get(eachEnv)!= null){
                                num_neighbours++;
                                count = count + hash_collect.get(date).get(eachEnv);
                            }
                        }
                        j=doubleHandle(j+0.01);
                    }i=doubleHandle(i+0.01);
                }
                listofLongs[0] = count;
                System.out.println(count);
                listofLongs[1] = num_neighbours;
                map_value.put(env, listofLongs);
            }
            result_map.put(date, map_value);
        }
        return result_map;
    }
}
//create the encompassed struccture file
class Encompas{
    Envelope this_envelope;
    String this_date;
    double this_val;
    public Encompas(Envelope env,String date,double val){
        this.this_envelope=env;
        this.this_date=date;
        this.this_val=val;
    }
}