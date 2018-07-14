package com.db.wisdom.model;

import java.util.List;

public class TopAuthorsResponse {

	private String author_name;

	private String uid;

	private Integer story_count;

	private Long totalpvs;

	private Long totaluvs;

	private Integer mpvs;

	private Integer wpvs;

	private Integer muvs;

	private Integer wuvs;

	private Long currentMonthPvs;
	//avg story count per day
	private Long avgStoryCount;

	private Long avgPvs;

	private Long avgUvs;

	private Double sharability;

	private Long shares;
	
	private Long sessions;


	private List<StoryDetail>  author_stories;
	
	public String getAuthor_name() {
		return author_name;
	}

	public void setAuthor_name(String author_name) {
		this.author_name = author_name;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public Integer getStory_count() {
		return story_count;
	}

	public void setStory_count(Integer story_count) {
		this.story_count = story_count;
	}

	public Long getTotalpvs() {
		return totalpvs;
	}

	public void setTotalpvs(Long totalpvs) {
		this.totalpvs = totalpvs;
	}

	public Integer getMpvs() {
		return mpvs;
	}

	public void setMpvs(Integer mpvs) {
		this.mpvs = mpvs;
	}

	public Integer getWpvs() {
		return wpvs;
	}

	public void setWpvs(Integer wpvs) {
		this.wpvs = wpvs;
	}

	public Long getCurrentMonthPvs() {
		return currentMonthPvs;
	}

	public void setCurrentMonthPvs(Long currentMonthPvs) {
		this.currentMonthPvs = currentMonthPvs;
	}

	public Long getAvgStoryCount() {
		return avgStoryCount;
	}

	public void setAvgStoryCount(Long avgStoryCount) {
		this.avgStoryCount = avgStoryCount;
	}

	public Long getAvgPvs() {
		return avgPvs;
	}

	public void setAvgPvs(Long avgPvs) {
		this.avgPvs = avgPvs;
	}

	public Long getTotaluvs() {
		return totaluvs;
	}

	public void setTotaluvs(Long totaluvs) {
		this.totaluvs = totaluvs;
	}

	public Long getAvgUvs() {
		return avgUvs;
	}

	public void setAvgUvs(Long avgUvs) {
		this.avgUvs = avgUvs;
	}

	public Integer getMuvs() {
		return muvs;
	}

	public void setMuvs(Integer muvs) {
		this.muvs = muvs;
	}

	public Integer getWuvs() {
		return wuvs;
	}

	public void setWuvs(Integer wuvs) {
		this.wuvs = wuvs;
	}

	public Double getSharability() {
		return sharability;
	}

	public void setSharability(Double sharability) {
		this.sharability = sharability;
	}

	public Long getShares() {
		return shares;
	}

	public void setShares(Long shares) {
		this.shares = shares;
	}

	public Long getSessions() {
		return sessions;
	}

	public void setSessions(Long sessions) {
		this.sessions = sessions;
	}

	public List<StoryDetail> getAuthor_stories() {
		return author_stories;
	}

	public void setAuthor_stories(List<StoryDetail> author_stories) {
		this.author_stories = author_stories;
	}
	
}
