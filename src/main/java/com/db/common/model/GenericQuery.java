package com.db.common.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class will hold attributes of all queries. Class is created to avoid too
 * many model classed to support different queries.
 * 
 * @author hanish
 *
 */
public class GenericQuery {

	private String startDate;
	private String endDate;
	private String hosts;
	private String editorName;
	private String editorId;

	private int from = 0;
	private int count = 100;
	private String storyid = "";
	private String tracker = "";
	
	private String userId = "";

	private Map<String,List<String>> include= new HashMap<>();
	private Map<String,List<String>> exclude= new HashMap<>();

	public Map<String, List<String>> getInclude() {
		return include;
	}

	public void setInclude(Map<String, List<String>> include) {
		this.include = include;
	}

	public Map<String, List<String>> getExclude() {
		return exclude;
	}

	public void setExclude(Map<String, List<String>> exclude) {
		this.exclude = exclude;
	}

	public String getStoryid() {
		return storyid;
	}

	public void setStoryid(String storyid) {
		this.storyid = storyid;
	}
	
	public String getTracker() {
		return tracker;
	}

	public void setTracker(String tracker) {
		this.tracker = tracker;
	}


	/**
	 * Brand Id for impression_logs index
	 */
	private int brandId=0;
	/**
	 * Supported values: month-wise, day-wise
	 */
	private String queryMode;
	
	//private int host;
	private String intervalFormat;

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

	public String getHosts() {
		return hosts;
	}

	public void setHosts(String hosts) {
		this.hosts = hosts;
	}

	public int getFrom() {
		return from;
	}

	public void setFrom(int from) {
		this.from = from;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getEditorName() {
		return editorName;
	}

	public void setEditorName(String editorName) {
		this.editorName = editorName;
	}

	public String getEditorId() {
		return editorId;
	}

	public void setEditorId(String editorId) {
		this.editorId = editorId;
	}

	public String getQueryMode() {
		return queryMode;
	}

	public void setQueryMode(String queryMode) {
		this.queryMode = queryMode;
	}

	public String getIntervalFormat() {
		return intervalFormat;
	}

	public void setIntervalFormat(String intervalFormat) {
		this.intervalFormat = intervalFormat;
	}

	public int getBrandId() {
		return brandId;
	}

	public void setBrandId(int brandId) {
		this.brandId = brandId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

}
