package com.db.cricket.model;

import com.google.gson.annotations.SerializedName;

public class ThisOver {

	@SerializedName("T")
	private String t;

	@SerializedName("B")
	private Integer b;

	public String getT() {
		return t;
	}

	public void setT(String t) {
		this.t = t;
	}

	public Integer getB() {
		return b;
	}

	public void setB(Integer b) {
		this.b = b;
	}
}
