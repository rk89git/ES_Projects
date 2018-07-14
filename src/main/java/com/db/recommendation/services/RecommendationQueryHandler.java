package com.db.recommendation.services;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.springframework.stereotype.Service;

import com.db.common.exception.DBAnalyticsException;
import com.db.common.model.UserPersonalizationQuery;
import com.db.recommendation.model.RecQuery;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Service
public class RecommendationQueryHandler {

	private RecommendationQueryExecutor recommendedQueryExecutorService = new RecommendationQueryExecutor();

	private static Logger log = LogManager.getLogger(RecommendationQueryHandler.class);

	private Gson gson = new GsonBuilder().setPrettyPrinting().create();

	/**
	 * This API is used for Related article 
	 * 
	 * @param jsonData
	 * @return
	 */
	public String getArticleStories(String jsonData) {
		RecQuery query = gson.fromJson(jsonData, RecQuery.class);
		if (StringUtils.isBlank(query.getStory_id())) {
			throw new DBAnalyticsException("Invalid query request. Mandatory attributes: story_id ");
		}

		if (StringUtils.isBlank(query.getBrand_id())) {
			throw new DBAnalyticsException("Invalid query request. Mandatory attributes: brand_id ");
		}

		if (query.getSize() == 0) {
			throw new DBAnalyticsException("Invalid size  for  recommendation : " + query.getSize());
		}
		return gson.toJson(recommendedQueryExecutorService.getRelatedArticles(query));
	}

	
	/**
	 * This API is used for User article 
	 * 
	 * @param jsonData
	 * @return
	 */
	public String getUserStories(String jsonData) {
		RecQuery query = gson.fromJson(jsonData, RecQuery.class);
		

		if (StringUtils.isBlank(query.getBrand_id())) {
			throw new DBAnalyticsException("Invalid query request. Mandatory attributes: brand_id ");
		}

		if (query.getSize() == 0) {
			throw new DBAnalyticsException("Invalid size  for  recommendation : " + query.getSize());
		}
		return gson.toJson(recommendedQueryExecutorService.getUserArticles(query));
	}
	
	/**
	 * This API is used for colloborating filtering 
	 * 
	 * @param jsonData
	 * @return
	 */
	public String getCollaborativeRecommendation(String jsonData) {
		RecQuery query = gson.fromJson(jsonData, RecQuery.class);
		
		if (StringUtils.isBlank(query.getStory_id())) {
			throw new DBAnalyticsException("Invalid query request. Mandatory attributes: story_id ");
		}


		if (StringUtils.isBlank(query.getBrand_id())) {
			throw new DBAnalyticsException("Invalid query request. Mandatory attributes: brand_id ");
		}

		if (query.getSize() == 0) {
			throw new DBAnalyticsException("Invalid size  for  recommendation : " + query.getSize());
		}
		return gson.toJson(recommendedQueryExecutorService.getcollaborativefilteredArticles(query));
	}
	
	public List<String> getKeywordRecommendation(String jsonData) {
		UserPersonalizationQuery query = gson.fromJson(jsonData, UserPersonalizationQuery.class);
		return recommendedQueryExecutorService.getKeywordRecommendation(query);
	}
	
	
	
	public static void main(String[] args) throws ElasticsearchException, ParseException {
		// QueryHandlerService handlerService=new QueryHandlerService();
		// handlerService.getTopAttribute("{\"session_id\":\"0dad6110-18a3-42be-a86b-b2565349f06e\",\"startDate\":\"2015-07-22\",\"endDate\":\"2015-07-22\",\"attribute\":\"classificationid\"}");

		String jsonData = "{'cat_id':8313,'hosts':'16','storyid': '11470534','sound':'silent','message': 'Shrey Testing Notification','rec_type':'2','location':'Noida','push_cat_id':1}";
		//RecommendationQueryHandler qhs = new RecommendationQueryHandler();
		//qhs.pushNotification(jsonData);
	}
	
	
}
