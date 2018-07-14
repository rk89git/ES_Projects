package com.db.common.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;

import com.db.common.exception.DBAnalyticsException;

public class DateUtil {

	private static Logger log = LogManager.getLogger(DateUtil.class);

	public static String getFormattedDateForIndexName(String date) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			return sdf.format(sdf.parse(date)).replaceAll("-", "_");
		} catch (Exception e) {
			System.out.println("Could not get formatted date. Using default logic to format date");
			return date.replaceAll("-", "_");
		}
	}

	public static String getCurrentDate() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
		Date date = new Date();

		return dateFormat.format(date);
	}

	public static String getPreviousDate() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
		Date date = new Date();
		Calendar calNext = Calendar.getInstance();

		calNext.setTime(date);
		calNext.add(Calendar.DATE, -1);

		return dateFormat.format(calNext.getTime());
		// return dateFormat.format(date);
	}

	public static String getPreviousDate(String date) {
		return DateUtil.getPreviousDate(date, "yyyy_MM_dd");
	}

	public static String getPreviousDate(String date, String strDateFormat) {
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat(strDateFormat);
			Date dateObj = dateFormat.parse(date);
			Calendar calNext = Calendar.getInstance();
			calNext.setTime(dateObj);
			calNext.add(Calendar.DATE, -1);
			
			return dateFormat.format(calNext.getTime());
		} catch (ParseException e) {
			throw new DBAnalyticsException(e.getMessage());
		}
	}
	
	public static String getDateOfPreviousMonth(String date, String strDateFormat) {
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat(strDateFormat);
			Date dateObj = dateFormat.parse(date);
			Calendar calNext = Calendar.getInstance();
			calNext.setTime(dateObj);
			calNext.add(Calendar.MONTH, -1);
			
			return dateFormat.format(calNext.getTime());
		} catch (ParseException e) {
			throw new DBAnalyticsException(e.getMessage());
		}
	}

	public static String getDateOfPreviousYear(String date, String strDateFormat) {
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat(strDateFormat);
			Date dateObj = dateFormat.parse(date);
			Calendar calNext = Calendar.getInstance();
			calNext.setTime(dateObj);
			calNext.add(Calendar.YEAR, -1);
			
			return dateFormat.format(calNext.getTime());
		} catch (ParseException e) {
			throw new DBAnalyticsException(e.getMessage());
		}
	}
	
	public static String getPreviousDate(String date, String strDateFormat, int dayCount) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(strDateFormat);
			Date dateObj = sdf.parse(date);
			Calendar calNext = Calendar.getInstance();
			calNext.setTime(dateObj);
			calNext.add(Calendar.DATE, dayCount);
			DateFormat dateFormat = new SimpleDateFormat(strDateFormat);
			return dateFormat.format(calNext.getTime());
		} catch (ParseException e) {
			log.error("Could not get previous date for date - " + date, e);
			throw new DBAnalyticsException(e.getMessage());
		}
	}

	public static String getPreviousDate(String date, int dayCount) {
		return DateUtil.getPreviousDate(date, "yyyy_MM_dd", dayCount);
	}

	public static String getCurrentDateTime() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		Date date = new Date();
		return dateFormat.format(date);
	}

	public static String addHoursToCurrentTime(int hours) {
		Date dateObj = new Date();
		Calendar calNext = Calendar.getInstance();
		calNext.setTime(dateObj);
		calNext.add(Calendar.HOUR, hours);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return dateFormat.format(calNext.getTime()).replace(" ", "T").concat("Z");
	}

	public static String addMinutesToCurrentTime(int minutes) {
		Date dateObj = new Date();
		Calendar calNext = Calendar.getInstance();
		calNext.setTime(dateObj);
		calNext.add(Calendar.MINUTE, minutes);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return dateFormat.format(calNext.getTime()).replace(" ", "T").concat("Z");
	}

	public static String addHoursToTime(String time, int hours) {
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date dateObj = dateFormat.parse(time.replace("T", " ").replace("Z", ""));
			Calendar calNext = Calendar.getInstance();
			calNext.setTime(dateObj);
			calNext.add(Calendar.HOUR, hours);
			return dateFormat.format(calNext.getTime()).replace(" ", "T").concat("Z");
		} catch (ParseException e) {
			throw new DBAnalyticsException(e.getMessage());
		}

	}

	public static int getTimedifferenceInSec(String startTime, String endTime) {
		Long diff = 0L;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date startDate = sdf.parse(startTime.replace("T", " ").replace("Z", ""));
			Date endDate = sdf.parse(endTime.replace("T", " ").replace("Z", ""));
			diff = (endDate.getTime() - startDate.getTime()) / 1000;
		} catch (ParseException e) {
			throw new DBAnalyticsException(e.getMessage());
		}
		return diff.intValue();
	}

	public static int getNumDaysBetweenDates(String startDate, String endDate) {
		final long DAY_IN_MILLIS = 1000 * 60 * 60 * 24;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cStart = Calendar.getInstance();
		int diffInDays = -1;
		try {
			cStart.setTime(sdf.parse(startDate));
			Calendar cEnd = Calendar.getInstance();
			cEnd.setTime(sdf.parse(endDate));
			diffInDays = (int) ((cEnd.getTimeInMillis() - cStart.getTimeInMillis()) / DAY_IN_MILLIS);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return diffInDays;
	}

	/**
	 * Get list of dates for n days
	 * 
	 * @param days
	 * @return
	 * @throws ParseException
	 */
	public static List<String> getDates(int days) throws ParseException {
		if (days > 90) {
			throw new DBAnalyticsException("Invalid days interval. Days must be less than 90 days.");
		}
		String currentDate = DateUtil.getCurrentDate();
		List<String> list = new ArrayList<String>();
		list.add(currentDate.replaceAll("_", "-"));
		for (int i = 0; i < days; i++) {
			currentDate = DateUtil.getPreviousDate(currentDate);
			list.add(currentDate.replaceAll("_", "-"));
		}
		System.out.println(list);
		return list;
	}

	public static List<String> getDates(String startDate, String endDate) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date dateFrom = sdf.parse(startDate);
		Date dateTo = sdf.parse(endDate);
		if (dateFrom.compareTo(dateTo) > 0) {
			throw new DBAnalyticsException("Failure: End date can not be before start date.");
		}
		Date currDate = (Date) dateFrom.clone();
		List<String> list = new ArrayList<String>();
		while (currDate.compareTo(dateTo) <= 0) {
			DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
			list.add(dateFormat.format(currDate));
			Calendar calNext = Calendar.getInstance();
			calNext.setTime(currDate);
			calNext.add(Calendar.DATE, 1);
			currDate = calNext.getTime();
		}
		return list; // "realtime_"+String.valueOf(yyyy) + "_" +
		// String.valueOf("0"+mm) + "_" + String.valueOf(dd);
	}

	public static String getFirstDateOfMonth() throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.DAY_OF_MONTH, 1);
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		String date = df.format(cal.getTime());
		return date;
	}
	
	public static int getNumberOfDaysInCurrentMonth() throws ParseException {
		Calendar cal = Calendar.getInstance();
		int days = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
		return days;
	}
	
	public static int getCurrentDayOfMonth() throws ParseException {
		Calendar cal = Calendar.getInstance();
		int day = cal.get(Calendar.DAY_OF_MONTH);
		return day;
	}

	public static int getHour(String startTime) {
		int diff = 0;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			Date currDate = sdf.parse(startTime);
			Calendar calNext = Calendar.getInstance();
			calNext.setTime(currDate);
			diff = calNext.get(Calendar.HOUR_OF_DAY);

		} catch (ParseException e) {
			throw new DBAnalyticsException(e.getMessage());
		}
		return diff;
	}

	public static int getWeekDay(String startDate) {
		int day = 0;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date currDate = sdf.parse(startDate);
			Calendar calNext = Calendar.getInstance();
			calNext.setTime(currDate);
			day = calNext.get(Calendar.DAY_OF_WEEK);

		} catch (ParseException e) {
			throw new DBAnalyticsException(e.getMessage());
		}
		return day;
	}

	public static List<String> getWeekDayDates(String startDate, String endDate, int weekDay) {
		ArrayList<String> dates = new ArrayList<>();

		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			Calendar scal = Calendar.getInstance();
			scal.setTime(dateFormat.parse(startDate));
			Calendar ecal = Calendar.getInstance();
			ecal.setTime(dateFormat.parse(endDate));
			while (scal.getTimeInMillis() <= ecal.getTimeInMillis()) {
				if (scal.get(Calendar.DAY_OF_WEEK) == weekDay) {
					dates.add(dateFormat.format(scal.getTime()));
				}
				if (dates.size() > 0)
					scal.add(Calendar.DATE, 7);
				else
					scal.add(Calendar.DATE, 1);
			}
			System.out.println(dates);
		} catch (ParseException e) {
			throw new DBAnalyticsException(e.getMessage());
		}
		return dates;
	}
	
	public static int getYear(String startDate) {
		int year = 0;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date currDate = sdf.parse(startDate);
			Calendar calNext = Calendar.getInstance();
			calNext.setTime(currDate);
			year = calNext.get(Calendar.YEAR);
		} catch (ParseException e) {
			throw new DBAnalyticsException(e.getMessage());
		}
		return year;
	}

	/**
	 * Get the current date using the date format provided.
	 * 
	 * @param format
	 *            the java date format string
	 * 
	 * @return the current date formatted using format.
	 */
	public static String getCurrentDate(String format) {
		DateFormat dateFormat = new SimpleDateFormat(format);
		Date date = new Date();
		return dateFormat.format(date);
	}

	/**
	 * Get date hours ahead of current date based on the date format provided.
	 * 
	 * @param hours
	 *            the number of hours to add.
	 * @param format
	 *            the java date format string
	 * 
	 * @return the formatted date, hours ahead of current date.
	 */
	public static String addHoursToCurrentTime(int hours, String format) {
		Date dateObj = new Date();
		Calendar calNext = Calendar.getInstance();
		calNext.setTime(dateObj);
		calNext.add(Calendar.HOUR, hours);
		DateFormat dateFormat = new SimpleDateFormat(format);
		return dateFormat.format(calNext.getTime()).replace(" ", "T").concat("Z");
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// DateUtil utils=new DateUtil();
		// String date=utils.getCurrentDate();
		/*
		 * int diff =
		 * DateUtil.getTimedifferenceInSec(DateUtil.addHoursToCurrentTime(-2),
		 * DateUtil.getCurrentDateTime()); System.out.println(diff);
		 */
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.DAY_OF_MONTH, 1);
		DateFormat df = new SimpleDateFormat("dd-MM-yyyy");
		String date1 = df.format(cal.getTime());
		// System.out.println(date1);

		System.out.println(DateUtil.getDateOfPreviousYear("2017-12-15", "yyyy-MM-dd"));
		
	}

}
