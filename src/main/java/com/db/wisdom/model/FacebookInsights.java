package com.db.wisdom.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FacebookInsights {

	private Integer all_scrolls_avg;
	
	private Integer all_view_dur_avg;
	
	private Long total_reach;

	private Long unique_reach;

	private Integer shares;

	private Long link_clicks;

	// ctr = ((link_clicks/total_reach)*100)
	private Double ctr;

	private Integer story_count;

	private String created_datetime;

	private String title;

	private String storyid;

	private String bhaskarstoryid;

	private String channel_slno;

	private String url;

	private String picture;

	private Long links_total_reach;

	private Long links_unique_reach;

	private Long links_clicked;

	private Double links_ctr;

	private Integer links_count;

	private Long photos_total_reach;

	private Long photos_unique_reach;

	private Long photos_clicked;

	private Double photos_ctr;

	private Integer photos_count;

	private String photo;

	private Integer videos_total_reach;

	private Integer videos_unique_reach;

	private Integer videos_clicked;

	private Double videos_ctr;

	private Integer videos_count;

	private Map<String,Object> categoryInsights;

	private String ga_cat_name;

	private Integer reaction_wow;

	private Integer reaction_thankful;

	private Integer reaction_haha;

	private Integer reaction_love;

	private Integer reaction_sad;

	private Integer reaction_angry;

	private Integer hide_all_clicks;

	private Integer hide_clicks;

	private Integer report_spam_clicks;

	private Integer likes;

	private Integer comments;

	private Integer links_reaction_wow;

	private Integer links_reaction_thankful;

	private Integer links_reaction_haha;

	private Integer links_reaction_love;

	private Integer links_reaction_sad;

	private Integer links_reaction_angry;

	private Integer links_hide_all_clicks;

	private Integer links_hide_clicks;

	private Integer links_report_spam_clicks;

	private Integer links_likes;

	private Integer links_comments;

	private Integer links_shares;

	private Integer photos_reaction_wow;

	private Integer photos_reaction_thankful;

	private Integer photos_reaction_haha;

	private Integer photos_reaction_love;

	private Integer photos_reaction_sad;

	private Integer photos_reaction_angry;

	private Integer photos_hide_all_clicks;

	private Integer photos_hide_clicks;

	private Integer photos_report_spam_clicks;

	private Integer photos_likes;

	private Integer photos_comments;

	private Integer photos_shares;

	private Integer videos_reaction_wow;

	private Integer videos_reaction_thankful;

	private Integer videos_reaction_haha;

	private Integer videos_reaction_love;

	private Integer videos_reaction_sad;

	private Integer videos_reaction_angry;

	private Integer videos_hide_all_clicks;

	private Integer videos_hide_clicks;

	private Integer videos_report_spam_clicks;

	private Integer videos_likes;

	private Integer videos_comments;

	private Integer videos_shares;

	private Boolean is_popular = false;

	private String permalink_url;

	private Integer weekDay;

	private Integer popular_count = 0;

	private Integer not_popular_count;

	private Integer popularity_score;	

	private String status_type;

	private Integer ia_clicks;

	private Integer video_views;

	private Integer unique_video_views;

	private Integer ia_story_count;

	private Double shareability;

	private Double links_shareability ;

	private Double photos_shareability;

	private Double videos_shareability;

	private Map<String, Object> probMap;

	private String top_range;

	private Double top_prob;

	private Long prev_total_reach;

	private Long total_reach_difference;

	private String prev_top_range;

	private Integer total_shares_difference;

	private Integer total_engagement_difference;

	private Integer sharesPerStoryCount;

	private Integer post_impressions_unique;

	private Integer post_impressions_paid;

	private Double sharability1;

	private Double sharability2;

	private Double sharability3;

	private Double ctr1;

	private Double ctr2;

	private Double ctr3;
	
	private Integer link_post_impressions_unique;

	private Integer link_post_impressions_paid;

	private Double link_sharability1;

	private Double link_sharability2;

	private Double link_sharability3;

	private Double link_ctr1;

	private Double link_ctr2;

	private Double link_ctr3;
	
	private Integer photo_post_impressions_unique;

	private Integer photo_post_impressions_paid;

	private Double photo_sharability1;

	private Double photo_sharability2;

	private Double photo_sharability3;

	private Double photo_ctr1;

	private Double photo_ctr2;

	private Double photo_ctr3;
	
	private Integer video_post_impressions_unique;

	private Integer video_post_impressions_paid;

	private Double video_sharability1;

	private Double video_sharability2;

	private Double video_sharability3;

	private Double video_ctr1;

	private Double video_ctr2;

	private Double video_ctr3;

	private Long sessions;
	
	private Long pvs;
	
	private Double completion_rate;
	
	private String super_cat_name;
	
	private String cat_name;
	
	public Integer getAll_scrolls_avg() {
		return all_scrolls_avg;
	}

	public void setAll_scrolls_avg(Integer all_scrolls_avg) {
		this.all_scrolls_avg = all_scrolls_avg;
	}

	public Integer getAll_view_dur_avg() {
		return all_view_dur_avg;
	}

	public void setAll_view_dur_avg(Integer all_view_dur_avg) {
		this.all_view_dur_avg = all_view_dur_avg;
	}

	public String getPrev_top_range() {
		return prev_top_range;
	}

	public void setPrev_top_range(String prev_top_range) {
		this.prev_top_range = prev_top_range;
	}

	public Long getTotal_reach_difference() {
		return total_reach_difference;
	}

	public void setTotal_reach_difference(Long total_reach_difference) {
		this.total_reach_difference = total_reach_difference;
	}

	public Long getPrev_total_reach() {
		return prev_total_reach;
	}

	public void setPrev_total_reach(Long prev_total_reach) {
		this.prev_total_reach = prev_total_reach;
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

	public Integer getReaction_wow() {
		return reaction_wow;
	}

	public void setReaction_wow(Integer reaction_wow) {
		this.reaction_wow = reaction_wow;
	}

	public Integer getReaction_thankful() {
		return reaction_thankful;
	}

	public void setReaction_thankful(Integer reaction_thankful) {
		this.reaction_thankful = reaction_thankful;
	}

	public Integer getReaction_haha() {
		return reaction_haha;
	}

	public void setReaction_haha(Integer reaction_haha) {
		this.reaction_haha = reaction_haha;
	}

	public Integer getReaction_love() {
		return reaction_love;
	}

	public void setReaction_love(Integer reaction_love) {
		this.reaction_love = reaction_love;
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

	public Integer getHide_all_clicks() {
		return hide_all_clicks;
	}

	public void setHide_all_clicks(Integer hide_all_clicks) {
		this.hide_all_clicks = hide_all_clicks;
	}

	public Integer getHide_clicks() {
		return hide_clicks;
	}

	public void setHide_clicks(Integer hide_clicks) {
		this.hide_clicks = hide_clicks;
	}

	public Integer getReport_spam_clicks() {
		return report_spam_clicks;
	}

	public void setReport_spam_clicks(Integer report_spam_clicks) {
		this.report_spam_clicks = report_spam_clicks;
	}

	public String getPicture() {
		return picture;
	}

	public void setPicture(String picture) {
		this.picture = picture;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getCreated_datetime() {
		return created_datetime;
	}

	public void setCreated_datetime(String created_datetime) {
		this.created_datetime = created_datetime;
	}

	public Long getTotal_reach() {
		return total_reach;
	}

	public void setTotal_reach(Long total_reach) {
		this.total_reach = total_reach;
	}

	public Long getUnique_reach() {
		return unique_reach;
	}

	public void setUnique_reach(Long unique_reach) {
		this.unique_reach = unique_reach;
	}

	public Integer getShares() {
		return shares;
	}

	public void setShares(Integer shares) {
		this.shares = shares;
	}

	public Long getLink_clicks() {
		return link_clicks;
	}

	public void setLink_clicks(Long link_clicks) {
		this.link_clicks = link_clicks;
	}

	public Double getCtr() {
		return ctr;
	}

	public void setCtr(Double ctr) {
		this.ctr = ctr;
	}

	public String getStoryid() {
		return storyid;
	}

	public void setStoryid(String storyid) {
		this.storyid = storyid;
	}

	public String getChannel_slno() {
		return channel_slno;
	}

	public void setChannel_slno(String channel_slno) {
		this.channel_slno = channel_slno;
	}

	public Long getLinks_clicked() {
		return links_clicked;
	}

	public void setLinks_clicked(Long links_clicked) {
		this.links_clicked = links_clicked;
	}

	public Long getPhotos_clicked() {
		return photos_clicked;
	}

	public void setPhotos_clicked(Long photos_clicked) {
		this.photos_clicked = photos_clicked;
	}

	public Integer getVideos_clicked() {
		return videos_clicked;
	}

	public void setVideos_clicked(Integer videos_clicked) {
		this.videos_clicked = videos_clicked;
	}

	public Integer getStory_count() {
		return story_count;
	}

	public void setStory_count(Integer story_count) {
		this.story_count = story_count;
	}

	public Map<String,Object> getCategoryInsights() {
		return categoryInsights;
	}

	public void setCategoryInsights(Map<String,Object> categoryInsights) {
		this.categoryInsights = categoryInsights;
	}

	public Long getLinks_total_reach() {
		return links_total_reach;
	}

	public void setLinks_total_reach(Long links_total_reach) {
		this.links_total_reach = links_total_reach;
	}

	public Long getLinks_unique_reach() {
		return links_unique_reach;
	}

	public void setLinks_unique_reach(Long links_unique_reach) {
		this.links_unique_reach = links_unique_reach;
	}

	public Integer getLinks_count() {
		return links_count;
	}

	public void setLinks_count(Integer links_count) {
		this.links_count = links_count;
	}

	public Long getPhotos_total_reach() {
		return photos_total_reach;
	}

	public void setPhotos_total_reach(Long photos_total_reach) {
		this.photos_total_reach = photos_total_reach;
	}

	public Long getPhotos_unique_reach() {
		return photos_unique_reach;
	}

	public void setPhotos_unique_reach(Long photos_unique_reach) {
		this.photos_unique_reach = photos_unique_reach;
	}

	public Integer getPhotos_count() {
		return photos_count;
	}

	public void setPhotos_count(Integer photos_count) {
		this.photos_count = photos_count;
	}

	public Integer getVideos_total_reach() {
		return videos_total_reach;
	}

	public void setVideos_total_reach(Integer videos_total_reach) {
		this.videos_total_reach = videos_total_reach;
	}

	public Integer getVideos_unique_reach() {
		return videos_unique_reach;
	}

	public void setVideos_unique_reach(Integer videos_unique_reach) {
		this.videos_unique_reach = videos_unique_reach;
	}

	public Integer getVideos_count() {
		return videos_count;
	}

	public void setVideos_count(Integer videos_count) {
		this.videos_count = videos_count;
	}

	public Double getLinks_ctr() {
		return links_ctr;
	}

	public void setLinks_ctr(Double links_ctr) {
		this.links_ctr = links_ctr;
	}

	public Double getPhotos_ctr() {
		return photos_ctr;
	}

	public void setPhotos_ctr(Double photos_ctr) {
		this.photos_ctr = photos_ctr;
	}

	public Double getVideos_ctr() {
		return videos_ctr;
	}

	public void setVideos_ctr(Double videos_ctr) {
		this.videos_ctr = videos_ctr;
	}

	public String getPhoto() {
		return photo;
	}

	public void setPhoto(String photo) {
		this.photo = photo;
	}

	public String getGa_cat_name() {
		return ga_cat_name;
	}

	public void setGa_cat_name(String ga_cat_name) {
		this.ga_cat_name = ga_cat_name;
	}

	public Boolean getIs_popular() {
		return is_popular;
	}

	public void setIs_popular(Boolean is_popular) {
		this.is_popular = is_popular;
	}	

	public Integer getLinks_reaction_wow() {
		return links_reaction_wow;
	}

	public void setLinks_reaction_wow(Integer links_reaction_wow) {
		this.links_reaction_wow = links_reaction_wow;
	}

	public Integer getLinks_reaction_thankful() {
		return links_reaction_thankful;
	}

	public void setLinks_reaction_thankful(Integer links_reaction_thankful) {
		this.links_reaction_thankful = links_reaction_thankful;
	}

	public Integer getLinks_reaction_haha() {
		return links_reaction_haha;
	}

	public void setLinks_reaction_haha(Integer links_reaction_haha) {
		this.links_reaction_haha = links_reaction_haha;
	}

	public Integer getLinks_reaction_love() {
		return links_reaction_love;
	}

	public void setLinks_reaction_love(Integer links_reaction_love) {
		this.links_reaction_love = links_reaction_love;
	}

	public Integer getLinks_reaction_sad() {
		return links_reaction_sad;
	}

	public void setLinks_reaction_sad(Integer links_reaction_sad) {
		this.links_reaction_sad = links_reaction_sad;
	}

	public Integer getLinks_reaction_angry() {
		return links_reaction_angry;
	}

	public void setLinks_reaction_angry(Integer links_reaction_angry) {
		this.links_reaction_angry = links_reaction_angry;
	}

	public Integer getLinks_hide_all_clicks() {
		return links_hide_all_clicks;
	}

	public void setLinks_hide_all_clicks(Integer links_hide_all_clicks) {
		this.links_hide_all_clicks = links_hide_all_clicks;
	}

	public Integer getLinks_hide_clicks() {
		return links_hide_clicks;
	}

	public void setLinks_hide_clicks(Integer links_hide_clicks) {
		this.links_hide_clicks = links_hide_clicks;
	}

	public Integer getLinks_report_spam_clicks() {
		return links_report_spam_clicks;
	}

	public void setLinks_report_spam_clicks(Integer links_report_spam_clicks) {
		this.links_report_spam_clicks = links_report_spam_clicks;
	}

	public Integer getLinks_likes() {
		return links_likes;
	}

	public void setLinks_likes(Integer links_likes) {
		this.links_likes = links_likes;
	}

	public Integer getLinks_comments() {
		return links_comments;
	}

	public void setLinks_comments(Integer links_comments) {
		this.links_comments = links_comments;
	}

	public Integer getPhotos_reaction_wow() {
		return photos_reaction_wow;
	}

	public void setPhotos_reaction_wow(Integer photos_reaction_wow) {
		this.photos_reaction_wow = photos_reaction_wow;
	}

	public Integer getPhotos_reaction_thankful() {
		return photos_reaction_thankful;
	}

	public void setPhotos_reaction_thankful(Integer photos_reaction_thankful) {
		this.photos_reaction_thankful = photos_reaction_thankful;
	}

	public Integer getPhotos_reaction_haha() {
		return photos_reaction_haha;
	}

	public void setPhotos_reaction_haha(Integer photos_reaction_haha) {
		this.photos_reaction_haha = photos_reaction_haha;
	}

	public Integer getPhotos_reaction_love() {
		return photos_reaction_love;
	}

	public void setPhotos_reaction_love(Integer photos_reaction_love) {
		this.photos_reaction_love = photos_reaction_love;
	}

	public Integer getPhotos_reaction_sad() {
		return photos_reaction_sad;
	}

	public void setPhotos_reaction_sad(Integer photos_reaction_sad) {
		this.photos_reaction_sad = photos_reaction_sad;
	}

	public Integer getPhotos_reaction_angry() {
		return photos_reaction_angry;
	}

	public void setPhotos_reaction_angry(Integer photos_reaction_angry) {
		this.photos_reaction_angry = photos_reaction_angry;
	}

	public Integer getPhotos_hide_all_clicks() {
		return photos_hide_all_clicks;
	}

	public void setPhotos_hide_all_clicks(Integer photos_hide_all_clicks) {
		this.photos_hide_all_clicks = photos_hide_all_clicks;
	}

	public Integer getPhotos_hide_clicks() {
		return photos_hide_clicks;
	}

	public void setPhotos_hide_clicks(Integer photos_hide_clicks) {
		this.photos_hide_clicks = photos_hide_clicks;
	}

	public Integer getPhotos_report_spam_clicks() {
		return photos_report_spam_clicks;
	}

	public void setPhotos_report_spam_clicks(Integer photos_report_spam_clicks) {
		this.photos_report_spam_clicks = photos_report_spam_clicks;
	}

	public Integer getPhotos_likes() {
		return photos_likes;
	}

	public void setPhotos_likes(Integer photos_likes) {
		this.photos_likes = photos_likes;
	}

	public Integer getPhotos_comments() {
		return photos_comments;
	}

	public void setPhotos_comments(Integer photos_comments) {
		this.photos_comments = photos_comments;
	}

	public Integer getVideos_reaction_wow() {
		return videos_reaction_wow;
	}

	public void setVideos_reaction_wow(Integer videos_reaction_wow) {
		this.videos_reaction_wow = videos_reaction_wow;
	}

	public Integer getVideos_reaction_thankful() {
		return videos_reaction_thankful;
	}

	public void setVideos_reaction_thankful(Integer videos_reaction_thankful) {
		this.videos_reaction_thankful = videos_reaction_thankful;
	}

	public Integer getVideos_reaction_haha() {
		return videos_reaction_haha;
	}

	public void setVideos_reaction_haha(Integer videos_reaction_haha) {
		this.videos_reaction_haha = videos_reaction_haha;
	}

	public Integer getVideos_reaction_love() {
		return videos_reaction_love;
	}

	public void setVideos_reaction_love(Integer videos_reaction_love) {
		this.videos_reaction_love = videos_reaction_love;
	}

	public Integer getVideos_reaction_sad() {
		return videos_reaction_sad;
	}

	public void setVideos_reaction_sad(Integer videos_reaction_sad) {
		this.videos_reaction_sad = videos_reaction_sad;
	}

	public Integer getVideos_reaction_angry() {
		return videos_reaction_angry;
	}

	public void setVideos_reaction_angry(Integer videos_reaction_angry) {
		this.videos_reaction_angry = videos_reaction_angry;
	}

	public Integer getVideos_hide_all_clicks() {
		return videos_hide_all_clicks;
	}

	public void setVideos_hide_all_clicks(Integer videos_hide_all_clicks) {
		this.videos_hide_all_clicks = videos_hide_all_clicks;
	}

	public Integer getVideos_hide_clicks() {
		return videos_hide_clicks;
	}

	public void setVideos_hide_clicks(Integer videos_hide_clicks) {
		this.videos_hide_clicks = videos_hide_clicks;
	}

	public Integer getVideos_report_spam_clicks() {
		return videos_report_spam_clicks;
	}

	public void setVideos_report_spam_clicks(Integer videos_report_spam_clicks) {
		this.videos_report_spam_clicks = videos_report_spam_clicks;
	}

	public Integer getVideos_likes() {
		return videos_likes;
	}

	public void setVideos_likes(Integer videos_likes) {
		this.videos_likes = videos_likes;
	}

	public Integer getVideos_comments() {
		return videos_comments;
	}

	public void setVideos_comments(Integer videos_comments) {
		this.videos_comments = videos_comments;
	}

	public Integer getLinks_shares() {
		return links_shares;
	}

	public void setLinks_shares(Integer links_shares) {
		this.links_shares = links_shares;
	}

	public Integer getPhotos_shares() {
		return photos_shares;
	}

	public void setPhotos_shares(Integer photos_shares) {
		this.photos_shares = photos_shares;
	}

	public Integer getVideos_shares() {
		return videos_shares;
	}

	public void setVideos_shares(Integer videos_shares) {
		this.videos_shares = videos_shares;
	}

	public String getBhaskarstoryid() {
		return bhaskarstoryid;
	}

	public void setBhaskarstoryid(String bhaskarstoryid) {
		this.bhaskarstoryid = bhaskarstoryid;
	}

	public String getPermalink_url() {
		return permalink_url;
	}

	public void setPermalink_url(String permalink_url) {
		this.permalink_url = permalink_url;
	}

	public Integer getWeekDay() {
		return weekDay;
	}

	public void setWeekDay(Integer weekDay) {
		this.weekDay = weekDay;
	}	

	public Integer getPopular_count() {
		return popular_count;
	}

	public void setPopular_count(Integer popular_count) {
		this.popular_count = popular_count;
	}

	public Integer getNot_popular_count() {
		return not_popular_count;
	}

	public void setNot_popular_count(Integer not_popular_count) {
		this.not_popular_count = not_popular_count;
	}

	public Integer getPopularity_score() {
		return popularity_score;
	}

	public void setPopularity_score(Integer popularity_score) {
		this.popularity_score = popularity_score;
	}

	public String getStatus_type() {
		return status_type;
	}

	public void setStatus_type(String status_type) {
		this.status_type = status_type;
	}	

	public Integer getIa_clicks() {
		return ia_clicks;
	}

	public void setIa_clicks(Integer ia_clicks) {
		this.ia_clicks = ia_clicks;
	}	

	public Integer getIa_story_count() {
		return ia_story_count;
	}

	public void setIa_story_count(Integer ia_story_count) {
		this.ia_story_count = ia_story_count;
	}

	public Double getShareability() {
		return shareability;
	}

	public void setShareability(Double shareability) {
		this.shareability = shareability;
	}

	public Double getLinks_shareability() {
		return links_shareability;
	}

	public void setLinks_shareability(Double links_shareability) {
		this.links_shareability = links_shareability;
	}

	public Double getPhotos_shareability() {
		return photos_shareability;
	}

	public void setPhotos_shareability(Double photos_shareability) {
		this.photos_shareability = photos_shareability;
	}

	public Double getVideos_shareability() {
		return videos_shareability;
	}

	public void setVideos_shareability(Double videos_shareability) {
		this.videos_shareability = videos_shareability;
	}	

	public Integer getVideo_views() {
		return video_views;
	}

	public void setVideo_views(Integer video_views) {
		this.video_views = video_views;
	}

	public Integer getUnique_video_views() {
		return unique_video_views;
	}

	public void setUnique_video_views(Integer unique_video_views) {
		this.unique_video_views = unique_video_views;
	}	

	public Map<String, Object> getProbMap() {
		return probMap;
	}

	public void setProbMap(Map<String, Object> probMap) {
		this.probMap = probMap;
	}

	public String getTop_range() {
		return top_range;
	}

	public void setTop_range(String top_range) {
		this.top_range = top_range;
	}

	public Double getTop_prob() {
		return top_prob;
	}

	public void setTop_prob(Double top_prob) {
		this.top_prob = top_prob;
	}

	@Override
	public String toString() {
		return "FacebookInsights [total_reach=" + total_reach + ", unique_reach=" + unique_reach + ", shares=" + shares
				+ ", link_clicks=" + link_clicks + ", ctr=" + ctr + ", created_datetime=" + created_datetime
				+ ", title=" + title + ", url=" + url + "]";
	}

	public Integer getTotal_shares_difference() {
		return total_shares_difference;
	}

	public void setTotal_shares_difference(Integer total_shares_difference) {
		this.total_shares_difference = total_shares_difference;
	}

	public Integer getTotal_engagement_difference() {
		return total_engagement_difference;
	}

	public void setTotal_engagement_difference(Integer total_engagement_difference) {
		this.total_engagement_difference = total_engagement_difference;
	}

	public Integer getSharesPerStoryCount() {
		return sharesPerStoryCount;
	}

	public void setSharesPerStoryCount(Integer sharesPerStoryCount) {
		this.sharesPerStoryCount = sharesPerStoryCount;
	}

	public Integer getPost_impressions_unique() {
		return post_impressions_unique;
	}

	public void setPost_impressions_unique(Integer post_impressions_unique) {
		this.post_impressions_unique = post_impressions_unique;
	}

	public Integer getPost_impressions_paid() {
		return post_impressions_paid;
	}

	public void setPost_impressions_paid(Integer post_impressions_paid) {
		this.post_impressions_paid = post_impressions_paid;
	}

	public Double getSharability1() {
		return sharability1;
	}

	public void setSharability1(Double sharability1) {
		this.sharability1 = sharability1;
	}

	public Double getSharability2() {
		return sharability2;
	}

	public void setSharability2(Double sharability2) {
		this.sharability2 = sharability2;
	}

	public Double getSharability3() {
		return sharability3;
	}

	public void setSharability3(Double sharability3) {
		this.sharability3 = sharability3;
	}

	public Double getCtr1() {
		return ctr1;
	}

	public void setCtr1(Double ctr1) {
		this.ctr1 = ctr1;
	}

	public Double getCtr2() {
		return ctr2;
	}

	public void setCtr2(Double ctr2) {
		this.ctr2 = ctr2;
	}

	public Double getCtr3() {
		return ctr3;
	}

	public void setCtr3(Double ctr3) {
		this.ctr3 = ctr3;
	}

	public Integer getLink_post_impressions_unique() {
		return link_post_impressions_unique;
	}

	public void setLink_post_impressions_unique(Integer link_post_impressions_unique) {
		this.link_post_impressions_unique = link_post_impressions_unique;
	}

	public Integer getLink_post_impressions_paid() {
		return link_post_impressions_paid;
	}

	public void setLink_post_impressions_paid(Integer link_post_impressions_paid) {
		this.link_post_impressions_paid = link_post_impressions_paid;
	}

	public Double getLink_sharability1() {
		return link_sharability1;
	}

	public void setLink_sharability1(Double link_sharability1) {
		this.link_sharability1 = link_sharability1;
	}

	public Double getLink_sharability2() {
		return link_sharability2;
	}

	public void setLink_sharability2(Double link_sharability2) {
		this.link_sharability2 = link_sharability2;
	}

	public Double getLink_sharability3() {
		return link_sharability3;
	}

	public void setLink_sharability3(Double link_sharability3) {
		this.link_sharability3 = link_sharability3;
	}

	public Double getLink_ctr1() {
		return link_ctr1;
	}

	public void setLink_ctr1(Double link_ctr1) {
		this.link_ctr1 = link_ctr1;
	}

	public Double getLink_ctr2() {
		return link_ctr2;
	}

	public void setLink_ctr2(Double link_ctr2) {
		this.link_ctr2 = link_ctr2;
	}

	public Double getLink_ctr3() {
		return link_ctr3;
	}

	public void setLink_ctr3(Double link_ctr3) {
		this.link_ctr3 = link_ctr3;
	}

	public Integer getPhoto_post_impressions_unique() {
		return photo_post_impressions_unique;
	}

	public void setPhoto_post_impressions_unique(Integer photo_post_impressions_unique) {
		this.photo_post_impressions_unique = photo_post_impressions_unique;
	}

	public Integer getPhoto_post_impressions_paid() {
		return photo_post_impressions_paid;
	}

	public void setPhoto_post_impressions_paid(Integer photo_post_impressions_paid) {
		this.photo_post_impressions_paid = photo_post_impressions_paid;
	}

	public Double getPhoto_sharability1() {
		return photo_sharability1;
	}

	public void setPhoto_sharability1(Double photo_sharability1) {
		this.photo_sharability1 = photo_sharability1;
	}

	public Double getPhoto_sharability2() {
		return photo_sharability2;
	}

	public void setPhoto_sharability2(Double photo_sharability2) {
		this.photo_sharability2 = photo_sharability2;
	}

	public Double getPhoto_sharability3() {
		return photo_sharability3;
	}

	public void setPhoto_sharability3(Double photo_sharability3) {
		this.photo_sharability3 = photo_sharability3;
	}

	public Double getPhoto_ctr1() {
		return photo_ctr1;
	}

	public void setPhoto_ctr1(Double photo_ctr1) {
		this.photo_ctr1 = photo_ctr1;
	}

	public Double getPhoto_ctr2() {
		return photo_ctr2;
	}

	public void setPhoto_ctr2(Double photo_ctr2) {
		this.photo_ctr2 = photo_ctr2;
	}

	public Double getPhoto_ctr3() {
		return photo_ctr3;
	}

	public void setPhoto_ctr3(Double photo_ctr3) {
		this.photo_ctr3 = photo_ctr3;
	}

	public Integer getVideo_post_impressions_unique() {
		return video_post_impressions_unique;
	}

	public void setVideo_post_impressions_unique(Integer video_post_impressions_unique) {
		this.video_post_impressions_unique = video_post_impressions_unique;
	}

	public Integer getVideo_post_impressions_paid() {
		return video_post_impressions_paid;
	}

	public void setVideo_post_impressions_paid(Integer video_post_impressions_paid) {
		this.video_post_impressions_paid = video_post_impressions_paid;
	}

	public Double getVideo_sharability1() {
		return video_sharability1;
	}

	public void setVideo_sharability1(Double video_sharability1) {
		this.video_sharability1 = video_sharability1;
	}

	public Double getVideo_sharability2() {
		return video_sharability2;
	}

	public void setVideo_sharability2(Double video_sharability2) {
		this.video_sharability2 = video_sharability2;
	}

	public Double getVideo_sharability3() {
		return video_sharability3;
	}

	public void setVideo_sharability3(Double video_sharability3) {
		this.video_sharability3 = video_sharability3;
	}

	public Double getVideo_ctr1() {
		return video_ctr1;
	}

	public void setVideo_ctr1(Double video_ctr1) {
		this.video_ctr1 = video_ctr1;
	}

	public Double getVideo_ctr2() {
		return video_ctr2;
	}

	public void setVideo_ctr2(Double video_ctr2) {
		this.video_ctr2 = video_ctr2;
	}

	public Double getVideo_ctr3() {
		return video_ctr3;
	}

	public void setVideo_ctr3(Double video_ctr3) {
		this.video_ctr3 = video_ctr3;
	}

	public Long getSessions() {
		return sessions;
	}

	public void setSessions(Long sessions) {
		this.sessions = sessions;
	}

	public String getSuper_cat_name() {
		return super_cat_name;
	}

	public void setSuper_cat_name(String super_cat_name) {
		this.super_cat_name = super_cat_name;
	}

	public String getCat_name() {
		return cat_name;
	}

	public void setCat_name(String cat_name) {
		this.cat_name = cat_name;
	}

	public Long getPvs() {
		return pvs;
	}

	public void setPvs(Long pvs) {
		this.pvs = pvs;
	}

	public Double getCompletion_rate() {
		return completion_rate;
	}

	public void setCompletion_rate(Double completion_rate) {
		this.completion_rate = completion_rate;
	}	
}
