package com.db.wisdom.services;

import java.text.ParseException;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;

import com.db.common.exception.DBAnalyticsException;
import com.db.common.model.GenericQuery;
import com.db.wisdom.model.WisdomQuery;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class WisdomQueryHandlerService {

	private WisdomQueryExecutorService wisdomQueryExecutorService = new WisdomQueryExecutorService();

	private static Logger log = LogManager.getLogger(WisdomQueryHandlerService.class);

	private Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public String getTopAuthorsList(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getTopAuthorsList(query));
	}

	public String getAuthorsMonthlyData(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getAuthorsMonthlyData(query));
	}

	public String getAuthorStoriesList(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		if (query.getUid() == null) {
			log.error("Invalid uid");
			throw new DBAnalyticsException("Invalid uid");
		}

		return gson.toJson(wisdomQueryExecutorService.getAuthorStoriesList(query));
	}

	
	public String editorDashboardTopStories(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getEditorDashboard(query));
	}

	public String getFacebookInsights(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getFacebookInsightsForDate(query));
	}

	public String getTopStoryListing(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		/**
		 * Strange Issue: Intermediate response object to handle missing
		 * previous data issue in API response.
		 */
		String response = gson.toJson(wisdomQueryExecutorService.getTopStoryListing(query));
		return response;
	}

	public String getStoryDetail(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getStoryDetail(query));
	}

	public String getSlideWisePvs(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getSlideWisePvs(query));
	}

	public String getStoryPerformance(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getStoryPerformance(query));
	}

	public String getFacebookStoryPerformance(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getFacebookStoryPerformance(query));
	}

	public String getTrackerwisePerformanceGraph(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getTrackerwisePerformanceGraph(query));
	}

	public String getVersionwisePerformance(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getVersionwisePerformance(query));
	}

	public String getStoryTimeline(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getStoryTimeline(query));
	}

	public String getTrendingEntities(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getTrendingEntities(query));
	}
	
	public String getFacebookInsightsByInterval(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getFacebookInsightsByInterval(query));
	}	
	
	public String getFacebookInsightsByIntervalAndCategory(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getFacebookInsightsByIntervalAndCategory(query));
	}	
	
	public String getFacebookInsightsByCategory(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getFacebookInsightsByCategory(query));
	}	
	
	public String getFacebookInsightsByCategoryAndDay(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getFacebookInsightsByCategoryAndDay(query));
	}
	
	public String getFacebookInsightsForAutomation(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getFacebookInsightsForAutomation(query));
	}
	
	public String getTrendingEntitiesForSocialDecode(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getTrendingEntitiesForSocialDecode(query));
	}
	
	public String getEODFlickerData(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getEODFlickerData(query));
	}
	
	public String getAdMetricsData(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getAdMetricsData(query));
	}
	
	public String getUserSessionDetails(String jsonData) {
		GenericQuery query = gson.fromJson(jsonData, GenericQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getUserSessionDetails(query));
	}

	public String getUserSessionBuckets(String jsonData) {
		GenericQuery query = gson.fromJson(jsonData, GenericQuery.class);

		if (StringUtils.isBlank(query.getHosts())) {
			String errMsg = "Invalid Session Bucket request. Mandatory Parameters: hosts";
			System.out.println(errMsg);
			throw new DBAnalyticsException(errMsg);
		}

		return gson.toJson(wisdomQueryExecutorService.getUserSessionBuckets(query));
	}

	public String getUserSessionBucketsWithDetails(String jsonData) {
		GenericQuery query = gson.fromJson(jsonData, GenericQuery.class);

		if (StringUtils.isBlank(query.getHosts())) {
			String errMsg = "Invalid Session Bucket request. Mandatory Parameters: hosts";
			System.out.println(errMsg);
			throw new DBAnalyticsException(errMsg);
		}

		return gson.toJson(wisdomQueryExecutorService.getUserSessionBucketsWithDetails(query));
	}
	
	public Integer getSessionCountForUser(String sessionId) {
		return wisdomQueryExecutorService.getSessionCountForUser(sessionId);
	}

	public String getArticleDiscovery(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getArticleDiscovery(query));
	}
	
	public String getWidgetArticleDiscovery(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getWidgetArticleDiscovery(query));
	}
	
	public String getArticleFeedback(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getArticleFeedback(query));
	}
		
	public String getUserFrequencybyStory(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getUserFrequencybyStory(query));
	}
	
	public String getFacebookInsightsForDate(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getFacebookInsightsForDate(query));
	}
	
	public String getFacebookUcbFlag(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getFacebookUcbFlag(query));
	}
	
	public String getFacebookPageInsightsByInterval(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getFacebookPageInsightsByInterval(query));
	}
	
	public String getTrackerwiseStoryDetail(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getTrackerwiseStoryDetail(query));
	}
	
	public String getFbCompetitors(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getFbCompetitors(query));
	}
	
	public String getFbCompetitorsWithInterval(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getFbCompetitorsWithInterval(query));
	}
	
	public String getFbCompetitorsStories(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getFbCompetitorsStories(query));
	}
	
	public String getFbVelocity(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getFbVelocity(query));
	}
	
	public String get0HourFbVelocity(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.get0HourFbVelocity(query));
	}
	
	public String getSimilarStories(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getSimilarStories(query));
	}
	
	public String get0HourFbVelocityGraph(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.get0HourFbVelocityGraph(query));
	}
	
	public String getStoriesForCommentDashboard(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getStoriesForCommentDashboard(query));
	}
	
	public String getComments(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getComments(query));
	}
	
	public String getVideosCtr(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getVideosCtr(query));
	}
	
	public String getWidgetWiseVideosCtr(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getWidgetWiseVideosCtr(query));
	}
	public String getSharability(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		if (StringUtils.isBlank(query.getStoryid())) {
			log.error("Invalid storyid");
			throw new DBAnalyticsException("Invalid storyid");
		}
		return gson.toJson(wisdomQueryExecutorService.getSharability(query));
	}
	public String getKraReport(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getKraReport(query));
	}
	public String getStoriesForFlickerAutomation(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getStoriesForFlickerAutomation(query));
	}
	public String getAuditSiteData(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getAuditSiteData(query));
	}
	
	public String getMISReport(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getMISReports(query));
	}
	
	public String getAuthorsListForNewsLetter(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getAuthorsListForNewsLetter(query));
	}
	public String getDailyData(String jsonData) {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getDailyData(query));
	}
	public String getYTDReport(String jsonData) throws ParseException {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getYTDReport(query));
	}
	
	public String getKeywordBasedSimilarStories(String jsonData) throws ParseException {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getKeywordBasedSimilarStories(query));
	}
	
	public String getKeywordBasedSimilarStoriesCount(String jsonData) throws ParseException {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getKeywordBasedSimilarStoriesCount(query));
	}
	
	public String getKeywordBasedSimilarStoriesCountAlphabetical(String jsonData) throws ParseException {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getKeywordBasedSimilarStoriesCountAlphabetical(query));
	}
	
	public String getKeywordBasedSimilarStoriesCountByInterval(String jsonData) throws ParseException {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getKeywordBasedSimilarStoriesCountByInterval(query));
	}
	
	public String getTotalEngagementByInterval(String jsonData) throws ParseException {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getTotalEngagementByInterval(query));
	}
	
	public String getVideoReport(String jsonData) throws ParseException {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getVideoReport(query));
	}
	
	public String getNotificationSessions(String jsonData) throws ParseException {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getNotificationSessions(query));
	}
	public String getDataForSocialTotal(String startDate) throws Exception {
		WisdomQuery query = new WisdomQuery();		
		query.setStartDate(startDate);
		return gson.toJson(wisdomQueryExecutorService.getDataForSocialTotal(query));
	}
	public String getCompleteStoryDetail(String jsonData) throws ParseException {
		WisdomQuery query = gson.fromJson(jsonData, WisdomQuery.class);
		return gson.toJson(wisdomQueryExecutorService.getCompleteStoryDetail(query));
	}
}
