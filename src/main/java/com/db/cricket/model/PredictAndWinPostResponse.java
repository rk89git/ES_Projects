package com.db.cricket.model;

public class PredictAndWinPostResponse {

	private String status;

	private int response_id;

	private Object result;

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public int getResponse_id() {
		return response_id;
	}

	public void setResponse_id(int response_id) {
		this.response_id = response_id;
	}

	public Object getResult() {
		return result;
	}

	public void setResult(Object result) {
		this.result = result;
	}

	@Override
	public String toString() {
		return "PredictAndWinPostResponse [status=" + status + ", response_id=" + response_id + ", result="
				+ result + "]";
	}

}
