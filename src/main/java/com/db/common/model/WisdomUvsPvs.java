package com.db.common.model;

public class WisdomUvsPvs {

	private long uvs;
	private long pvs;
	private long activeuv;
	private long activepv;
	private double pgdepth;
	private double pgdepth_story;
	private String host;
	private long storyCount;

	public long getUvs() {
		return uvs;
	}

	public void setUvs(long uvs) {
		this.uvs = uvs;
	}

	public long getPvs() {
		return pvs;
	}

	public void setPvs(long pvs) {
		this.pvs = pvs;
	}

	public long getActiveuv() {
		return activeuv;
	}

	public void setActiveuv(long activeuv) {
		this.activeuv = activeuv;
	}

	public long getActivepv() {
		return activepv;
	}

	public void setActivepv(long activepv) {
		this.activepv = activepv;
	}

	public double getPgdepth() {
		return pgdepth;
	}

	public void setPgdepth(double pgdepth) {
		this.pgdepth = pgdepth;
	}

	public double getPgdepth_story() {
		return pgdepth_story;
	}

	public void setPgdepth_story(double pgdepth_story) {
		this.pgdepth_story = pgdepth_story;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public long getStoryCount() {
		return storyCount;
	}

	public void setStoryCount(long storyCount) {
		this.storyCount = storyCount;
	}

}
