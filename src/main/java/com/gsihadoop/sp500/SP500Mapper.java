package com.gsihadoop.sp500;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.gsihadoop.utils.DateUtilities;
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
				
				String yearlyAggKey = symbol + "," + record.getDate().substring(0,4);
				context.write(new Text(yearlyAggKey), new DoubleWritable(record.getStock_price_close()));
				
				String monthlyAggKey = symbol + "," + record.getDate().substring(0,4) + "," + record.getDate().substring(5,7);
				context.write(new Text(monthlyAggKey), new DoubleWritable(record.getStock_price_close()));
				
				Calendar weekDate = Calendar.getInstance();
				
				try {
					weekDate.setTime(DateUtilities.getDate(record.getDate()));
					String weeklyAggKey = symbol + "," + record.getDate().substring(0,4) + "," + record.getDate().substring(5,7) + "," + weekDate.get(Calendar.WEEK_OF_YEAR);
					context.write(new Text(weeklyAggKey), new DoubleWritable(record.getStock_price_close()));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
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
