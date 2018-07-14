package com.db.wisdom.product.model;

public class UserSession {

	private String session_count;

	private String user_id;

	private String user_count;

	public String getSession_count() {
		return session_count;
	}

	public void setSession_count(String session_count) {
		this.session_count = session_count;
	}

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public String getUser_count() {
		return user_count;
	}

	public void setUser_count(String user_count) {
		this.user_count = user_count;
	}

	@Override
	public String toString() {
		return "UserSession [session_count=" + session_count + ", user_id=" + user_id + ", user_count=" + user_count
				+ "]";
	}

}