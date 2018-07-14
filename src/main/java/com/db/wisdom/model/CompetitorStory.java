package com.db.wisdom.model;

public class CompetitorStory {

	public String title;
	public Double score;
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
	public String highlightedTitle;	
	public Integer stories_count;
	public Integer reaction_love;
	public Integer reaction_wow;
	public Integer reaction_haha;
	public Integer reaction_sad;
	public Integer reaction_angry;
	public Integer reaction_thankful;
	public Integer likes;
	public Integer comments;
	public Double sentimentScore;

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
	public Double getScore() {
		return score;
	}
	public void setScore(Double score) {
		this.score = score;
	}
	public Integer getStories_count() {
		return stories_count;
	}
	public void setStories_count(Integer storyCount) {
		this.stories_count = storyCount;
	}
	public String getHighlightedTitle() {
		return highlightedTitle;
	}
	public void setHighlightedTitle(String highlightedTitle) {
		this.highlightedTitle = highlightedTitle;
	}
	public Integer getReaction_love() {
		return reaction_love;
	}
	public void setReaction_love(Integer reaction_love) {
		this.reaction_love = reaction_love;
	}
	public Integer getReaction_wow() {
		return reaction_wow;
	}
	public void setReaction_wow(Integer reaction_wow) {
		this.reaction_wow = reaction_wow;
	}
	public Integer getReaction_haha() {
		return reaction_haha;
	}
	public void setReaction_haha(Integer reaction_haha) {
		this.reaction_haha = reaction_haha;
	}
	public Integer getReaction_sad() {
		return reaction_sad;
	}
	public void setReaction_sad(Integer reaction_sad) {
		this.reaction_sad = reaction_sad;
	}
	public Integer getReaction_angry() {
		return reaction_angry;
	}
	public void setReaction_angry(Integer reaction_angry) {
		this.reaction_angry = reaction_angry;
	}
	public Integer getReaction_thankful() {
		return reaction_thankful;
	}
	public void setReaction_thankful(Integer reaction_thankful) {
		this.reaction_thankful = reaction_thankful;
	}
	public Integer getLikes() {
		return likes;
	}
	public void setLikes(Integer likes) {
		this.likes = likes;
	}
	public Integer getComments() {
		return comments;
	}
	public void setComments(Integer comments) {
		this.comments = comments;
	}
	public Double getSentimentScore() {
		return sentimentScore;
	}
	public void setSentimentScore(Double sentimentScore) {
		this.sentimentScore = sentimentScore;
	}
	
}
