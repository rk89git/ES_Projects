package com.db.common.model;

public class RealtimeRecord {
	private String session_id;
	private Object storyid;
	private String title;
	private String url;
	private String image;
	private Object cat_id;
	private int host;
	private Object pcat_id;
	private int width;
	private int height;
	private Object dimension;
	private Object story_attribute;
	private String story_pubtime;
	private long pvs; 
	private String datetime;
	private Object nlp_nouns;
	
	
	public String getSession_id() {
		return session_id;
	}
	public void setSession_id(String session_id) {
		this.session_id = session_id;
	}
	public Object getStoryid() {
		return storyid;
	}
	public void setStoryid(Object storyid) {
		this.storyid = storyid;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
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
	public Object getCat_id() {
		return cat_id;
	}
	public void setCat_id(Object cat_id) {
		this.cat_id = cat_id;
	}
	
	
	public int getHost() {
		return host;
	}
	public void setHost(int host) {
		this.host = host;
	}
	public Object getPcat_id() {
		return pcat_id;
	}
	public void setPcat_id(Object pcat_id) {
		this.pcat_id = pcat_id;
	}
	public int getWidth() {
		return width;
	}
	public void setWidth(int width) {
		this.width = width;
	}
	public int getHeight() {
		return height;
	}
	public void setHeight(int height) {
		this.height = height;
	}
	public Object getDimension() {
		return dimension;
	}
	public void setDimension(Object dimension) {
		this.dimension = dimension;
	}
	public Object getStory_attribute() {
		return story_attribute;
	}
	public void setStory_attribute(Object story_attribute) {
		this.story_attribute = story_attribute;
	}
	public String getStory_pubtime() {
		return story_pubtime;
	}
	public void setStory_pubtime(String story_pubtime) {
		this.story_pubtime = story_pubtime;
	}
	public long getPvs() {
		return pvs;
	}
	public void setPvs(long pvs) {
		this.pvs = pvs;
	}
	public String getDatetime() {
		return datetime;
	}
	public void setDatetime(String datetime) {
		this.datetime = datetime;
	}
	public Object getNlp_nouns() {
		return nlp_nouns;
	}
	public void setNlp_nouns(Object nlp_nouns) {
		this.nlp_nouns = nlp_nouns;
	}
	@Override
	public String toString() {
		return "RealtimeRecord [session_id=" + session_id + ", storyid=" + storyid + ", title=" + title + ", url=" + url
				+ ", image=" + image + ", cat_id=" + cat_id + ", host=" + host + ", pcat_id=" + pcat_id + ", width="
				+ width + ", height=" + height + ", dimension=" + dimension + ", story_attribute=" + story_attribute
				+ ", story_pubtime=" + story_pubtime + ", pvs=" + pvs + ", datetime=" + datetime + ", nlp_nouns="
				+ nlp_nouns + "]";
	}
		
	
}
