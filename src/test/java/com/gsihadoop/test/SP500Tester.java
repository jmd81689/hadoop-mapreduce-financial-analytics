package com.gsihadoop.test;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.gsihadoop.sp500.SP500Mapper;
import com.gsihadoop.sp500.SP500Reducer;
 
public class SP500Tester {
	final static Charset ENCODING = StandardCharsets.UTF_8;
  MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
  //ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
  //MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
 
  @Before
  public void setUp() {
	  Configuration conf = new Configuration();
	  conf.set("sp500Path", "testdata/SP500.txt");
	  conf.set("testData", "testdata/NYSE_daily_prices_A.csv");
	  SP500Mapper mapper = new SP500Mapper();
	  //SP500Reducer reducer = new SP500Reducer();
	  mapDriver = MapDriver.newMapDriver(mapper);
	  mapDriver.withConfiguration(conf);
	  //reduceDriver = ReduceDriver.newReduceDriver(reducer);
	  //mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }
 
  @Test
  public void testMapper() throws IOException {
	  //Path path = Paths.get("testdata/NYSE_daily_prices_A.csv");
	  //System.out.println(Files.readAllLines(path, ENCODING));
	 
	 
	  //BufferedReader br = new BufferedReader(new FileReader("testdata/NYSE_daily_prices_A.csv"));
	  Configuration conf = mapDriver.getConfiguration();
	  BufferedReader br = new BufferedReader(new FileReader(conf.get("testData")));
	  String line = "";
	  while((line = br.readLine()) != null){
		  //ADD LOGIC HERE FOR CREATING INPUT KV
		  mapDriver.withInput(new LongWritable(), new Text(line));
	  }
	 // mapDriver.withInput(new LongWritable(), new Text("NYSE,ABT,2010-02-08,53.85,53.95,53.31,53.35,7130400,53.35"));
	  List<Pair<Text, DoubleWritable>> results = mapDriver.run();
	  for(Iterator<Pair<Text,DoubleWritable>> resultsItr = results.iterator(); resultsItr.hasNext();){
		  System.out.println(resultsItr.next().toString());
	  }
	  
	/*mapDriver.withInput(new LongWritable(), new Text(
        "655209;1;796764372490213;804422938115889;6"));
    mapDriver.withOutput(new Text("6"), new IntWritable(1));
    mapDriver.runTest();(*/
	  br.close();
  }
 
  @Test
  public void testReducer() {
    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));
    //reduceDriver.withInput(new Text("6"), values);
    //reduceDriver.withOutput(new Text("6"), new IntWritable(2));
    //reduceDriver.runTest();
  }
}