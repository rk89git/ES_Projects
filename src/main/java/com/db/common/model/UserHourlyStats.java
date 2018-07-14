package com.db.common.model;

import java.util.ArrayList;

public class UserHourlyStats {

	public String getSession_id() {
		return session_id;
	}
	public void setSession_id(String session_id) {
		this.session_id = session_id;
	}
	public ArrayList<StatsDate> getStatsDates() {
		return statsDates;
	}
	public void setStatsDates(ArrayList<StatsDate> statsDates) {
		this.statsDates = statsDates;
	}
	private String session_id;
	private ArrayList<StatsDate> statsDates;	
}
