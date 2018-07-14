package com.db.common.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;

import com.db.common.constants.Indexes;
import com.db.common.exception.DBAnalyticsException;

public class IndexUtils {

	private static Logger log = LogManager.getLogger(IndexUtils.class);

	/**
	 * Get list of indexes between two dates.
	 * 
	 * @param startDate
	 *            - inclusive
	 * @param endDate
	 *            - inclusive
	 * @return
	 * @throws ParseException
	 */
	public static String[] getIndexes(String startDate, String endDate) {
		return getDailyIndexes("realtime_",startDate, endDate);
	}
	
	public static String[] getDailyIndexes(String indexPrefix, String startDate, String endDate) {
		List<String> list = new ArrayList<String>();
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date dateFrom = sdf.parse(startDate);
			Date dateTo = sdf.parse(endDate);

			if (dateFrom.compareTo(dateTo) > 0) {
				throw new DBAnalyticsException("Failure: End date can not be before start date.");
			}
			Date currDate = (Date) dateFrom.clone();

			while (currDate.compareTo(dateTo) <= 0) {
				DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
				list.add(indexPrefix + dateFormat.format(currDate));
				Calendar calNext = Calendar.getInstance();
				calNext.setTime(currDate);
				calNext.add(Calendar.DATE, 1);
				currDate = calNext.getTime();
			}
		} catch (ParseException e) {
			log.error("Error in deriving index names.", e);
		}
		String[] indexArray = list.toArray(new String[list.size()]);
		return indexArray;
	}

	public static List<String> getAppIndexes(String startDate, String endDate) throws ParseException {
		return Arrays.asList(getDailyIndexes("app_",startDate, endDate));
	}
	
	public static String[] getYearlyIndexes(String index, String startDate, String endDate){
		List<String> list = new ArrayList<String>();
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date dateFrom = sdf.parse(startDate.replaceAll("_", "-"));
			Date dateTo = sdf.parse(endDate.replaceAll("_", "-"));

			if (dateFrom.compareTo(dateTo) > 0) {
				throw new DBAnalyticsException("Failure: End date can not be before start date.");
			}
			Date currDate = (Date) dateFrom.clone();
			DateFormat dateFormat = new SimpleDateFormat("yyyy");			
			while (dateFormat.format(currDate).compareTo(dateFormat.format(dateTo)) <= 0) {
				list.add(index +"_"+ dateFormat.format(currDate));
				Calendar calNext = Calendar.getInstance();
				calNext.setTime(currDate);
				calNext.add(Calendar.YEAR, 1);
				currDate = calNext.getTime();
				
			}
		} catch (ParseException e) {
			log.error("Error in deriving index names.", e);
		}
		String[] indexArray = list.toArray(new String[list.size()]);
		return indexArray;
	}
		
	public static String[] getMonthlyIndexes(String index, String startDate, String endDate){
		List<String> list = new ArrayList<String>();
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date dateFrom = sdf.parse(startDate.replaceAll("_", "-"));
			Date dateTo = sdf.parse(endDate.replaceAll("_", "-"));

			if (dateFrom.compareTo(dateTo) > 0) {
				throw new DBAnalyticsException("Failure: End date can not be before start date.");
			}
			
			Date currDate = (Date) dateFrom.clone();
			DateFormat dateFormat = new SimpleDateFormat("yyyy_MM");			
			while (dateFormat.format(currDate).compareTo(dateFormat.format(dateTo)) <= 0) {
				list.add(index +"_"+ dateFormat.format(currDate));
				Calendar calNext = Calendar.getInstance();
				calNext.setTime(currDate);
				calNext.add(Calendar.MONTH, 1);
				currDate = calNext.getTime();
			}
			
		} catch (ParseException e) {
			log.error("Error in deriving index names.", e);
		}
		String[] indexArray = list.toArray(new String[list.size()]);
		return indexArray;
	}
	
	public static String getYearlyIndex(String index)
	{
		String indexName = "";
		try{
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date date = sdf.parse(DateUtil.getCurrentDate().replaceAll("_", "-"));
			DateFormat dateFormat = new SimpleDateFormat("yyyy");
			indexName = index+"_"+dateFormat.format(date);
		} catch (ParseException e) {
			log.error("Error in deriving index names.", e);
		}
		return indexName;
	}
	
	public static String getMonthlyIndex(String index)
	{
		String indexName = "";
		try{
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date date = sdf.parse(DateUtil.getCurrentDate().replaceAll("_", "-"));
			DateFormat dateFormat = new SimpleDateFormat("yyyy_MM");
			indexName = index+"_"+dateFormat.format(date);
		} catch (ParseException e) {
			log.error("Error in deriving index names.", e);
		}
		return indexName;
	}

	public static void main(String[] args) throws ParseException {

		System.out.println(IndexUtils.getYearlyIndexes(Indexes.STORY_DETAIL, DateUtil.getCurrentDate(), DateUtil.getCurrentDate()));
	}
}
