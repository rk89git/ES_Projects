package com.db.wisdom.product.model;

public class CompetitorStory {

	public String title;
	public String storyid;
	public String picture;
	public String photo;
	public String competitor;
	public String created_datetime;
	public String status_type;
	public Integer total_engagement;
	public Double engagementPerformance=0.0;
	public Long shares;
	public Double sharesPerformance=0.0;
	public String permalink_url;
	public String channel_slno;	
	public Long video_views;
	
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getCompetitor() {
		return competitor;
	}
	public void setCompetitor(String competitor) {
		this.competitor = competitor;
	}
	public String getCreated_datetime() {
		return created_datetime;
	}
	public void setCreated_datetime(String created_datetime) {
		this.created_datetime = created_datetime;
	}
	public String getStatus_type() {
		return status_type;
	}
	public void setStatus_type(String status_type) {
		this.status_type = status_type;
	}
	public Integer getTotal_engagement() {
		return total_engagement;
	}
	public void setTotal_engagement(Integer engagement) {
		this.total_engagement = engagement;
	}
	public Double getEngagementPerformance() {
		return engagementPerformance;
	}
	public void setEngagementPerformance(Double engagementPerformance) {
		this.engagementPerformance = engagementPerformance;
	}
	public Long getShares() {
		return shares;
	}
	public void setShares(Long shares) {
		this.shares = shares;
	}
	public Double getSharesPerformance() {
		return sharesPerformance;
	}
	public void setSharesPerformance(Double sharesPerformance) {
		this.sharesPerformance = sharesPerformance;
	}
	public String getStoryid() {
		return storyid;
	}
	public void setStoryid(String storyid) {
		this.storyid = storyid;
	}
	public String getPicture() {
		return picture;
	}
	public void setPicture(String picture) {
		this.picture = picture;
	}
	public String getPhoto() {
		return photo;
	}
	public void setPhoto(String photo) {
		this.photo = photo;
	}
	public String getPermalink_url() {
		return permalink_url;
	}
	public void setPermalink_url(String permalink_url) {
		this.permalink_url = permalink_url;
	}
	public String getChannel_slno() {
		return channel_slno;
	}
	public void setChannel_slno(String channel_slno) {
		this.channel_slno = channel_slno;
	}
	public Long getVideo_views() {
		return video_views;
	}
	public void setVideo_views(Long video_views) {
		this.video_views = video_views;
	}
	
		
}
