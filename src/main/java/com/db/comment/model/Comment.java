package com.db.comment.model;

public class Comment {

	private String channel_slno;

	private String datetime;

	private String description;

	private String email;

	private String event;

	private String id;

	private String image = "";

	private String in_reply_to_comment_id;

	private String in_reply_to_user_id;

	private long like_count;

	private String name;

	private String post_id;

	private long reply_count;

	private long report_abuse_count;

	private String url;

	private String user_id;

	private boolean isReply;

	private String modified_description;

	private boolean isAgree;

	public String storyid;
	
	public Integer host;
	
	public Integer getHost() {
		return host;
	}

	public void setHost(Integer host) {
		this.host = host;
	}

	public String getImage() {
		return image;
	}

	public void setImage(String image) {
		this.image = image;
	}

	public boolean isAgree() {
		return isAgree;
	}

	public void setAgree(boolean isAgree) {
		this.isAgree = isAgree;
	}

	public String getModified_description() {
		return modified_description;
	}

	public void setModified_description(String modified_description) {
		this.modified_description = modified_description;
	}

	public boolean isReply() {
		return isReply;
	}

	public void setReply(boolean isReply) {
		this.isReply = isReply;
	}

	public String getChannel_slno() {
		return channel_slno;
	}

	public void setChannel_slno(String channel_slno) {
		this.channel_slno = channel_slno;
	}

	public String getDatetime() {
		return datetime;
	}

	public void setDatetime(String datetime) {
		this.datetime = datetime;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getEvent() {
		return event;
	}

	public void setEvent(String event) {
		this.event = event;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getIn_reply_to_comment_id() {
		return in_reply_to_comment_id;
	}

	public void setIn_reply_to_comment_id(String in_reply_to_comment_id) {
		this.in_reply_to_comment_id = in_reply_to_comment_id;
	}

	public String getIn_reply_to_user_id() {
		return in_reply_to_user_id;
	}

	public void setIn_reply_to_user_id(String in_reply_to_user_id) {
		this.in_reply_to_user_id = in_reply_to_user_id;
	}

	public long getLike_count() {
		return like_count;
	}

	public void setLike_count(long like_count) {
		this.like_count = like_count;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPost_id() {
		return post_id;
	}

	public void setPost_id(String post_id) {
		this.post_id = post_id;
	}

	public long getReply_count() {
		return reply_count;
	}

	public void setReply_count(long reply_count) {
		this.reply_count = reply_count;
	}

	public long getReport_abuse_count() {
		return report_abuse_count;
	}

	public void setReport_abuse_count(long report_abuse_count) {
		this.report_abuse_count = report_abuse_count;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public String getStoryid() {
		return storyid;
	}

	public void setStoryid(String storyid) {
		this.storyid = storyid;
	}
	

}
