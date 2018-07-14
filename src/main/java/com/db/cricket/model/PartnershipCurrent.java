
package com.db.cricket.model;

import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.gson.annotations.SerializedName;

public class PartnershipCurrent {

	@SerializedName("Runs")
	private String runs;

	@SerializedName("Balls")
	private String balls;

	@SerializedName("Batsmen")
	private List<Batsman> batsmen = null;

	public String getRuns() {
		return runs;
	}

	public void setRuns(String runs) {
		this.runs = runs;
	}

	public String getBalls() {
		return balls;
	}

	public void setBalls(String balls) {
		this.balls = balls;
	}

	public List<Batsman> getBatsmen() {
		return batsmen;
	}

	public void setBatsmen(List<Batsman> batsmen) {
		this.batsmen = batsmen;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

}
