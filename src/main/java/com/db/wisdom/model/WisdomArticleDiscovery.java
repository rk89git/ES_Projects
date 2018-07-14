package com.db.wisdom.model;

public class WisdomArticleDiscovery {
	
	
	private Double ctr;

	private String title;

	private String storyid;

	private String channel_slno;

	private String url;

	private String image;

	private String published_date;
	
	private Long clicks;
    
	private String widget_name;
	
	private String widget_page_name;
	
	private Long impression;
	
	private Long ref_impression;
	
	private Long ref_view;
	
	
	
	public String getWidget_page_name() {
		return widget_page_name;
	}

	public void setWidget_page_name(String widget_page_name) {
		this.widget_page_name = widget_page_name;
	}

	public Long getRef_impression() {
		return ref_impression;
	}

	public void setRef_impression(Long ref_impression) {
		this.ref_impression = ref_impression;
	}

	public Long getRef_view() {
		return ref_view;
	}

	public void setRef_view(Long ref_view) {
		this.ref_view = ref_view;
	}

	public String getWidget_name() {
		return widget_name;
	}

	public void setWidget_name(String widget_name) {
		this.widget_name = widget_name;
	}

	public Long getImpression() {
		return impression;
	}

	public void setImpression(Long impression) {
		this.impression = impression;
	}

	public Double getCtr() {
		return ctr;
	}

	public void setCtr(Double ctr) {
		this.ctr = ctr;
	}
	
	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
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

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getImage() {
		return image;
	}

	public void setImage(String image) {
		this.image = image;
	}

	public String getPublished_date() {
		return published_date;
	}

	public void setPublished_date(String published_date) {
		this.published_date = published_date;
	}

	public Long getClicks() {
		return clicks;
	}

	public void setclicks(Long clicks) {
		this.clicks = clicks;
	}
	
	
	

}
