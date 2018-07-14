package com.db.cricket.model;

public class BidDetailsSummaryResponse {

	private String userId ;
	private String ticketId;
	private String matchId ;
	private String bidType ;
	private String bidTypeId ;
	private int prediction ;
	private int coinsBid ;
	
	
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getTicketId() {
		return ticketId;
	}
	public void setTicketId(String ticketId) {
		this.ticketId = ticketId;
	}
	public String getMatchId() {
		return matchId;
	}
	public void setMatchId(String matchId) {
		this.matchId = matchId;
	}
	public String getBidType() {
		return bidType;
	}
	public void setBidType(String bidType) {
		this.bidType = bidType;
	}
	public String getBidTypeId() {
		return bidTypeId;
	}
	public void setBidTypeId(String bidTypeId) {
		this.bidTypeId = bidTypeId;
	}
	public int getPrediction() {
		return prediction;
	}
	public void setPrediction(int prediction) {
		this.prediction = prediction;
	}
	public int getCoinsBid() {
		return coinsBid;
	}
	public void setCoinsBid(int coinsBid) {
		this.coinsBid = coinsBid;
	}
	
}
