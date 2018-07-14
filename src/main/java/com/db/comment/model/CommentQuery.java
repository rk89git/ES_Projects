package com.db.comment.model;

public class CommentQuery {

	private String channel_slno;
	
	private boolean isReply;
	
	private String id;
	
	private int from;
	
	private int size = 10;

	private String post_id;
	
	private String datetime;
	
	private String startDate;
	
	private String endDate;
	
	private String host;
	
	private String storyid;
	
	private Boolean isAgree;
	
	public Boolean isAgree() {
		return isAgree;
	}

	public void setAgree(Boolean isAgree) {
		this.isAgree = isAgree;
	}

	public String getStoryid() {
		return storyid;
	}

	public void setStoryid(String storyid) {
		this.storyid = storyid;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getDatetime() {
		return datetime;
	}

	public void setDatetime(String datetime) {
		this.datetime = datetime;
	}

	public String getChannel_slno() {
		return channel_slno;
	}

	public void setChannel_slno(String channel_slno) {
		this.channel_slno = channel_slno;
	}

	public boolean isReply() {
		return isReply;
	}

	public void setReply(boolean isReply) {
		this.isReply = isReply;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getFrom() {
		return from;
	}

	public void setFrom(int from) {
		this.from = from;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public String getPost_id() {
		return post_id;
	}

	public void setPost_id(String post_id) {
		this.post_id = post_id;
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
	
}
