package com.db.common.model;

import java.util.ArrayList;
import java.util.List;

public class ArticleParams {

	private String hosts;
	
	private String catId;
	
	private String pCatId;
	
	private String startDate;
	
	private String endDate;
	
	private String from;
	
	private int count = 15;
	
	private String excludeStories;
	
	private String includeStories;
	
	private String orderField;
	
	private String order;
	
	private String topic;
	
	private String inputStoriesId;
	
	private int inCount;
	

	public int getInCount() {
		return inCount;
	}

	public void setInCount(int inCount) {
		this.inCount = inCount;
	}

	private List<String> tracker = new ArrayList<>();
	
	private List<String> excludeTracker = new ArrayList<>();

	public String getHosts() {
		return hosts;
	}

	public void setHosts(String hosts) {
		this.hosts = hosts;
	}

	public String getCatId() {
		return catId;
	}

	public void setCatId(String catid) {
		this.catId = catid;
	}

	public String getpCatId() {
		return pCatId;
	}

	public void setpCatId(String pcatid) {
		this.pCatId = pcatid;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	public String getFrom() {
		return from;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getExcludeStories() {
		return excludeStories;
	}

	public void setExcludeStories(String excludeStories) {
		this.excludeStories = excludeStories;
	}

	public String getIncludeStories() {
		return includeStories;
	}

	public void setIncludeStories(String includeStories) {
		this.includeStories = includeStories;
	}

	public String getOrderField() {
		return orderField;
	}

	public void setOrderField(String orderField) {
		this.orderField = orderField;
	}

	public String getOrder() {
		return order;
	}

	public void setOrder(String order) {
		this.order = order;
	}

	public List<String> getTracker() {
		return tracker;
	}

	public void setTracker(List<String> tracker) {
		this.tracker = tracker;
	}

	public List<String> getExcludeTracker() {
		return excludeTracker;
	}

	public void setExcludeTracker(List<String> excludeTracker) {
		this.excludeTracker = excludeTracker;
	}

	public String getTopic() {
		return topic;
	}
	
	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getInputStoriesId() {
		return inputStoriesId;
	}

	public void setInputStoriesId(String inputStoriesId) {
		this.inputStoriesId = inputStoriesId;
	}

	
}
