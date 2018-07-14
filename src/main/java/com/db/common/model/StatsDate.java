package com.db.common.model;

import java.util.ArrayList;

public class StatsDate {
	public ArrayList<Hours> getHours() {
		return hours;
	}

	public void setHours(ArrayList<Hours> hours) {
		this.hours = hours;
	}
	
	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	private ArrayList<Hours> hours;
	private String date;
		
}
