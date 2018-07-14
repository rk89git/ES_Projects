
package com.db.cricket.model;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.gson.annotations.SerializedName;

public class Stats {

	@SerializedName("Style")
	private String style;

	@SerializedName("Average")
	private String average;

	@SerializedName("Strikerate")
	private String strikerate;

	@SerializedName("Economyrate")
	private String economyrate;

	@SerializedName("Runs")
	private String runs;

	public String getStyle() {
		return style;
	}

	public void setStyle(String style) {
		this.style = style;
	}

	public String getAverage() {
		return average;
	}

	public void setAverage(String average) {
		this.average = average;
	}

	public String getStrikerate() {
		return strikerate;
	}

	public void setStrikerate(String strikerate) {
		this.strikerate = strikerate;
	}

	public String getRuns() {
		return runs;
	}

	public void setRuns(String runs) {
		this.runs = runs;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

}
