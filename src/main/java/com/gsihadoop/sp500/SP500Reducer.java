package com.gsihadoop.sp500;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.gsihadoop.utils.DateUtilities;

public class SP500Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	private MultipleOutputs<Text, DoubleWritable> multipleOutputs;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		multipleOutputs  = new MultipleOutputs(context);
	}
	
	/**
	 * The `Reducer` method.
	 * 
	 * @param key
	 *            - Input key - Name of the region
	 * @param values
	 *            - Input Value - Iterator over the values
	 * @param context
	 *            - Used for collecting output
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		AggregatorType aggType;
		
		String[] bigKey = key.toString().split(",");
		String symbol = bigKey[0];
		String year = bigKey[1];
		aggType = AggregatorType.YEARLY;
		try{
			String month = bigKey[2];
			aggType = AggregatorType.MONTHLY;
			String weekOfYear = bigKey[3];
			aggType = AggregatorType.WEEKLY;
		} catch(NullPointerException npe){
			
		} catch(ArrayIndexOutOfBoundsException aob){
			
		}
		
		double numValues = 0.0;
		double sumValues = 0.0;
		
		for(DoubleWritable dwValue : values){
			double value = dwValue.get();
			sumValues+=value;
			numValues++;
		}
		
		double result = sumValues/numValues;
		
		switch(aggType){
			case YEARLY:
				multipleOutputs.write("yearly", key, result, "yearly/part");
				break;
			case MONTHLY:
				multipleOutputs.write("monthly", key, result, "monthly/part");
				break;
			case WEEKLY:
				multipleOutputs.write("weekly", key, result, "weekly/part");
				break;
				
		}
		/*
		 * mos.write("text", , key, new Text("Hello"));
 mos.write("seq", LongWritable(1), new Text("Bye"), "seq_a");
 mos.write("seq", LongWritable(2), key, new Text("Chau"), "seq_b");
 mos.write(key, new Text("value"), generateFileName(key, new Text("value")));
		 * 
		 * 
		 * 
		 * 
		 * double aggregateYearClose = 0;
		double lowYear = Double.MAX_VALUE;
		
		String[] keyValue = (key.toString()).split(",");
		String symbol = keyValue[0];
		String year = keyValue[1];
		
		for (Text value : values) {
			String[] stringValues = (value.toString()).split(",");
			if (stringValues.length == 2){
				String dateString = stringValues[0];
				double close = Double.parseDouble(stringValues[1]);
				
				Calendar recordDate = Calendar.getInstance();
				try {
					recordDate.setTime(DateUtilities.getDate(dateString));
				} catch (ParseException e) {
					e.printStackTrace();
				}
				
				
				//Year
				aggregateYearClose += close;
				
				// Week
				context.write(key, new DoubleWritable());
				
				//Month
				context.write(key, new DoubleWritable());
			}
		}
		// Year
		context.write(key, new DoubleWritable(aggregateYearClose));*/
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}
