package com.db.cricket.model;

public class MatchWinnerDetailPostRequest {

	private String user_id ;
	private String match_id ;
	private int match_no ;
	private String match_name ;
	private String match_datetime ;
	private int winner_type ;
	
	public String getUser_id() {
		return user_id;
	}
	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}
	public String getMatch_id() {
		return match_id;
	}
	public void setMatch_id(String match_id) {
		this.match_id = match_id;
	}
	public int getMatch_no() {
		return match_no;
	}
	public void setMatch_no(int match_no) {
		this.match_no = match_no;
	}
	public String getMatch_name() {
		return match_name;
	}
	public void setMatch_name(String match_name) {
		this.match_name = match_name;
	}
	public String getMatch_datetime() {
		return match_datetime;
	}
	public void setMatch_datetime(String match_datetime) {
		this.match_datetime = match_datetime;
	}
	public int getWinner_type() {
		return winner_type;
	}
	public void setWinner_type(int winner_type) {
		this.winner_type = winner_type;
	}
	
	
	

	   
	   
}
