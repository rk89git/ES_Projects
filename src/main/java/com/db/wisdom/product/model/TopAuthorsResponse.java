package com.db.wisdom.product.model;

public class TopAuthorsResponse {

	private String author;

	private String author_id;

	private Integer story_count;

	private Long totalpvs;
	
	private Long totalupvs;

	private Integer mpvs;

	private Integer wpvs;
	
	private Integer mupvs;

	private Integer wupvs;
	
	private Long currentMonthPvs;
	//avg story count per day
	private Long avgStoryCount;
	
	private Long avgPvs;
	
	private Long avgUpvs;

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author_name) {
		this.author = author_name;
	}

	public String getAuthor_id() {
		return author_id;
	}

	public void setAuthor_id(String uid) {
		this.author_id = uid;
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

	public Long getTotalupvs() {
		return totalupvs;
	}

	public void setTotalupvs(Long totaluvs) {
		this.totalupvs = totaluvs;
	}
	
	public Long getAvgUpvs() {
		return avgUpvs;
	}

	public void setAvgUpvs(Long avgUpvs) {
		this.avgUpvs = avgUpvs;
	}

	public Integer getMupvs() {
		return mupvs;
	}

	public void setMupvs(Integer muvs) {
		this.mupvs = muvs;
	}

	public Integer getWupvs() {
		return wupvs;
	}

	public void setWupvs(Integer wuvs) {
		this.wupvs = wuvs;
	}

	
}
