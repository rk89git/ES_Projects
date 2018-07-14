package com.db.cricket.model;

public class NotificationIdResponse {

	private String status;

	private String notification_id;

	private String result;

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getNotification_id() {
		return notification_id;
	}

	public void setNotification_id(String notification_id) {
		this.notification_id = notification_id;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	@Override
	public String toString() {
		return "NotificationIdResponse [status=" + status + ", notification_id=" + notification_id + ", result="
				+ result + "]";
	}

}
