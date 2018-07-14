package com.db.comment.model;

public class CommentNotification {

	private String userName;

	private String url;

	private String id;

	private String msgType;

	private String storyid;

	private String event;

	private String sessionId;

	private Integer postId;

	private Boolean isReply;

	public Boolean getIsReply() {
		return isReply;
	}

	public void setIsReply(Boolean isReply) {
		this.isReply = isReply;
	}

	public Integer getPostId() {
		return postId;
	}

	public void setPostId(Integer postId) {
		this.postId = postId;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getEvent() {
		return event;
	}

	public void setEvent(String event) {
		this.event = event;
	}

	public String getStoryid() {
		return storyid;
	}

	public void setStoryid(String storyid) {
		this.storyid = storyid;
	}

	public String getMsgType() {
		return msgType;
	}

	public void setMsgType(String msgType) {
		this.msgType = msgType;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	@Override
	public String toString() {
		return "CommentNotification [setUserName userName=" + userName + ", url=" + url + "]";
	}

	/**
	 * Method meant to provide for title strings for comment notifications.
	 */
	public String toNotificationString() {
		return userName + " ने आपके द्वारा किए गए कमेंट का जवाब दिया है।";
	}

	/**
	 * Method meant to provide for title strings for comment notifications when user
	 * follows a post.
	 */
	public String toFollowNotificationString() {
		return "" + userName + " ने आपके द्वारा फॉलो किये गए पोस्ट पर कमेंट किया है";
	}

	/**
	 * Method meant to provide for title strings for comment notifications when user
	 * likes a comment.
	 */
	public String toLikeCommentNotificationString() {
		return "Someone liked your comment";
	}

	/**
	 * Method meant to provide slug intro(notification body) for comment
	 * notifications.
	 */
	public String slugIntro() {
		return null;
	}

}
