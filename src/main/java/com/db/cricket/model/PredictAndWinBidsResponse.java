package com.db.cricket.model;

public class PredictAndWinBidsResponse {

	private String matchId;
	
	private String userId;
	
	private String vendorId;
	
	private String pVendorId;
	
	private String ticketId;
	
	private int coinsDeduct;
	
	private int noOfBidsAccepted;
	
	private int requestedBids;
	
	private int refundGems;

	
	/**
	 * @return the refundGems
	 */
	public int getRefundGems() {
		return refundGems;
	}

	/**
	 * @param refundGems the refundGems to set
	 */
	public void setRefundGems(int refundGems) {
		this.refundGems = refundGems;
	}

	/**
	 * @return the ticketId
	 */
	public String getTicketId() {
		return ticketId;
	}

	/**
	 * @param ticketId
	 *            the ticketId to set
	 */
	public void setTicketId(String ticketId) {
		this.ticketId = ticketId;
	}

	public String getMatchId() {
		return matchId;
	}

	public void setMatchId(String matchId) {
		this.matchId = matchId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getVendorId() {
		return vendorId;
	}

	public void setVendorId(String vendorId) {
		this.vendorId = vendorId;
	}

	public String getpVendorId() {
		return pVendorId;
	}

	public void setpVendorId(String pVendorId) {
		this.pVendorId = pVendorId;
	}

	public int getCoinsDeduct() {
		return coinsDeduct;
	}

	public void setCoinsDeduct(int coinsDeduct) {
		this.coinsDeduct = coinsDeduct;
	}

	public int getNoOfBidsAccepted() {
		return noOfBidsAccepted;
	}

	public void setNoOfBidsAccepted(int noOfBidsAccepted) {
		this.noOfBidsAccepted = noOfBidsAccepted;
	}

	public int getRequestedBids() {
		return requestedBids;
	}

	public void setRequestedBids(int requestedBids) {
		this.requestedBids = requestedBids;
	}
	
}
