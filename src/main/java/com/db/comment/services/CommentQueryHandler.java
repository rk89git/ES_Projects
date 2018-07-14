package com.db.comment.services;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.db.comment.model.CommentQuery;
import com.db.comment.model.CommentReport;
import com.db.common.exception.DBAnalyticsException;
import com.db.notification.v1.model.NotificationQuery;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Service
public class CommentQueryHandler {
	
	private Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
	@Autowired
	private CommentQueryExecutor commentQueryExecutor;

	
	public Map<String,Object> getComments(String jsonData) {
		CommentQuery query = gson.fromJson(jsonData, CommentQuery.class);
		if (StringUtils.isBlank(query.getChannel_slno())) {
			throw new DBAnalyticsException("Invalid getDbComments request. Mandatory attributes: channel_slno");
		}
		return commentQueryExecutor.getComments(query);
	}
	
	public List<CommentReport> getDayWiseReport(String jsonData) {
		CommentQuery query = gson.fromJson(jsonData, CommentQuery.class);
		return commentQueryExecutor.getDayWiseReport(query);
	}

	public Map<String, Object> getMostEngagedComment(String jsonData) {
		CommentQuery query = gson.fromJson(jsonData, CommentQuery.class);
		return commentQueryExecutor.getMostEngagedComment(query);
	}
	
	public Map<String, Object> getMostEngagedArticle(String jsonData) {
		CommentQuery query = gson.fromJson(jsonData, CommentQuery.class);
		return commentQueryExecutor.getMostEngagedArticle(query);
	}

	public Map<String, Object> getTopReviews(NotificationQuery notificationQuery) {
		return commentQueryExecutor.getTopReviews(notificationQuery);
	}

	public Map<String, Object> getRecentReviews(NotificationQuery notificationQuery) {
		return commentQueryExecutor.getRecentReviews(notificationQuery);
	}
	
	public Map<String,Object> getCommentsById(String jsonData) {
		CommentQuery query = gson.fromJson(jsonData, CommentQuery.class);
		return commentQueryExecutor.getCommentsById(query);
	}
	
	public Map<String,Object> getAgreeOrDisagreeComments(String jsonData) {
		CommentQuery query = gson.fromJson(jsonData, CommentQuery.class);		
		return commentQueryExecutor.getAgreeOrDisagreeComments(query);
	}
	
	public String pushSpamWords(Map<String, Object> jsonData) throws Exception{
		return commentQueryExecutor.pushSpamWords(jsonData);
	}
	
	public String deleteComment(Map<String, Object> jsonData){
		try{
			return commentQueryExecutor.deleteComment(jsonData);
		}catch(Exception e){
			throw new DBAnalyticsException(e);
		}
	}
	
	public String deleteUser(Map<String, Object> jsonData){
		try{
			return commentQueryExecutor.deleteUser(jsonData);
		}catch(Exception e){
			throw new DBAnalyticsException(e);
		}
	}
}
