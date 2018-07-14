package com.db.wisdom.product.services;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;

import com.db.wisdom.product.model.WisdomProductQuery;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class WisdomProductQueryHandlerService {

	private WisdomProductQueryExecutorService wisdomProductQueryExecutorService = new WisdomProductQueryExecutorService();

	private static Logger log = LogManager.getLogger(WisdomProductQueryHandlerService.class);

	private Gson gson = new GsonBuilder().setPrettyPrinting().create();

	
	public String getTopStoryListing(String jsonData) {
		WisdomProductQuery query = gson.fromJson(jsonData, WisdomProductQuery.class);
		/**
		 * Strange Issue: Intermediate response object to handle missing
		 * previous data issue in API response.
		 */
		String response = gson.toJson(wisdomProductQueryExecutorService.getTopStoryListing(query));
		return response;
	}	
	
	public String getAuthorsList(String jsonData) {
		WisdomProductQuery query = gson.fromJson(jsonData, WisdomProductQuery.class);
		String response = gson.toJson(wisdomProductQueryExecutorService.getAuthorsList(query));
		return response;
	}
	
	public String getTopAuthorsList(String jsonData) {
		WisdomProductQuery query = gson.fromJson(jsonData, WisdomProductQuery.class);
		String response = gson.toJson(wisdomProductQueryExecutorService.getTopAuthorsList(query));
		return response;
	}
	
	public String getAuthorsMonthlyData(String jsonData) {
		WisdomProductQuery query = gson.fromJson(jsonData, WisdomProductQuery.class);
		String response = gson.toJson(wisdomProductQueryExecutorService.getAuthorsMonthlyData(query));
		return response;
	}
	
	public String getStoryDetail(String jsonData) {
		WisdomProductQuery query = gson.fromJson(jsonData, WisdomProductQuery.class);
		String response = gson.toJson(wisdomProductQueryExecutorService.getStoryDetail(query));
		return response;
	}
	
	public String getVersionwisePerformance(String jsonData) {
		WisdomProductQuery query = gson.fromJson(jsonData, WisdomProductQuery.class);
		String response = gson.toJson(wisdomProductQueryExecutorService.getVersionwisePerformance(query));
		return response;
	}
	
	public String getStoryPerformance(String jsonData) {
		WisdomProductQuery query = gson.fromJson(jsonData, WisdomProductQuery.class);
		String response = gson.toJson(wisdomProductQueryExecutorService.getStoryPerformance(query));
		return response;
	}
	public String getTrackerwisePerformanceGraph(String jsonData) {
		WisdomProductQuery query = gson.fromJson(jsonData, WisdomProductQuery.class);
		String response = gson.toJson(wisdomProductQueryExecutorService.getTrackerwisePerformanceGraph(query));
		return response;
	}
	public String getSlideWisePvs(String jsonData) {
		WisdomProductQuery query = gson.fromJson(jsonData, WisdomProductQuery.class);
		String response = gson.toJson(wisdomProductQueryExecutorService.getSlideWisePvs(query));
		return response;
	}
	public String getStoryTimeline(String jsonData) {
		WisdomProductQuery query = gson.fromJson(jsonData, WisdomProductQuery.class);
		String response = gson.toJson(wisdomProductQueryExecutorService.getStoryTimeline(query));
		return response;
	}
	
	public String getFacebookInsightsByInterval(String jsonData) {
		WisdomProductQuery query = gson.fromJson(jsonData, WisdomProductQuery.class);
		return gson.toJson(wisdomProductQueryExecutorService.getFacebookInsightsByInterval(query));
	}
	
	public String getFacebookInsightsForDate(String jsonData) {
		WisdomProductQuery query = gson.fromJson(jsonData, WisdomProductQuery.class);
		return gson.toJson(wisdomProductQueryExecutorService.getFacebookInsightsForDate(query));
	}
	
	public String getFacebookInsightsByIntervalAndCategory(String jsonData) {
		WisdomProductQuery query = gson.fromJson(jsonData, WisdomProductQuery.class);
		return gson.toJson(wisdomProductQueryExecutorService.getFacebookInsightsByIntervalAndCategory(query));
	}	
	
	public String getFacebookInsightsByCategory(String jsonData) {
		WisdomProductQuery query = gson.fromJson(jsonData, WisdomProductQuery.class);
		return gson.toJson(wisdomProductQueryExecutorService.getFacebookInsightsByCategory(query));
	}
	
	public String getFacebookInsights(String jsonData) {
		WisdomProductQuery query = gson.fromJson(jsonData, WisdomProductQuery.class);
		return gson.toJson(wisdomProductQueryExecutorService.getFacebookInsights(query));
	}
		
}
