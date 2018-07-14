package com.db.common.model;

public class UserHistory {
	
	private String session_id;
	
	private String story_id;
	
	private String image;
	
	private String url;
	
	private String title;
	
	private String datetime;
	
	public String getSession_id() {
		return session_id;
	}

	public void setSession_id(String session_id) {
		this.session_id = session_id;
	}

	public String getStory_id() {
		return story_id;
	}

	public void setStory_id(String story_id) {
		this.story_id = story_id;
	}

	public String getImage() {
		return image;
	}

	public void setImage(String image) {
		this.image = image;
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

	public String getDatetime() {
		return datetime;
	}

	public void setDatetime(String datetime) {
		this.datetime = datetime;
	}

	@Override
	public String toString() {
		return "UserHistory [session_id=" + session_id + ", story_id="
				+ story_id + ", image=" + image + ", url=" + url + ", title="
				+ title + ", datetime=" + datetime + "]";
	}
	
}
