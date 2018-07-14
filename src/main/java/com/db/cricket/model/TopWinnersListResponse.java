package com.db.cricket.model;

import com.db.cricket.model.TopWinnersResponse;
import java.util.List ;

public class TopWinnersListResponse {
	private List<TopWinnersResponse> topWinners ;
    private List<LuckyWinnersResponse> luckyWinners ;
    

    private LuckyWinnersResponse userDetail;
    
    
     public List<TopWinnersResponse> getTopWinners() {
		return topWinners;
	}
	public void setTopWinners(List<TopWinnersResponse> topWinners) {
		this.topWinners = topWinners;
	}
	public List<LuckyWinnersResponse> getLuckyWinners() {
		return luckyWinners;
	}
	public void setLuckyWinners(List<LuckyWinnersResponse> luckyWinners) {
		this.luckyWinners = luckyWinners;
	}
	public LuckyWinnersResponse getUserDetail() {
		return userDetail;
	}
	public void setUserDetail(LuckyWinnersResponse user) {
		this.userDetail = user;
	}
	
	
	
}
