package com.gsihadoop.sp500;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.gsihadoop.utils.SP500List;
import com.gsihadoop.utils.StockData;

public class SP500Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context){
		Configuration conf = context.getConfiguration();
		
		Path path = Paths.get(conf.get("sp500Path"));
		String[] sp500List = SP500List.parseFile(path);
		HashSet<String> sp500HashSet = SP500List.getSP500HashSet();
		String line = value.toString();
		try {
			// May need to add firstline detection here
			StockData record = StockData.parse(line);
			String symbol = record.getStock_symbol();
			System.out.println(line);
			if (sp500HashSet.contains(symbol)){
				/*String outputKey = symbol + "," + record.getDate().substring(0,4);
				String outputValue = record.getDate()+ "," + record.getStock_price_close();
				context.write(new Text(outputKey), new Text(outputValue));*/
				
				String yearlyAggKey = symbol + "," + record.getDate().substring(0,4);
				//String yearlyAggValue = "" + record.getStock_price_close();
				//context.write(new Text(yearlyAggKey), new Text(yearlyAggValue));
				context.write(new Text(yearlyAggKey), new DoubleWritable(record.getStock_price_close()));
				String monthlyAggKey = symbol + "," + record.getDate().substring(0,7);
				//String monthlyAggValue = "" + record.getStock_price_close();
				//context.write(new Text(monthlyAggKey), new Text(monthlyAggValue));
				context.write(new Text(monthlyAggKey), new DoubleWritable(record.getStock_price_close()));
			}
		} catch(ArrayIndexOutOfBoundsException e){
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} 
	}
	
}
