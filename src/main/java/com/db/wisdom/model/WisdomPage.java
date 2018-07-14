package com.db.wisdom.model;

import java.util.List;

public class WisdomPage {

	private Long currentDayPvs = 0L;

	/*private Long previousDayPvs = 0L;
	
	private Long currentDayMPvs = 0L;

	private Long previousDayMPvs = 0L;
	
	private Long currentDayWPvs = 0L;

	private Long previousDayWPvs = 0L;
	
	private Long currentDayAPvs = 0L;

	private Long previousDayAPvs = 0L;
	
	private Long currentDayIPvs = 0L;

	private Long previousDayIPvs = 0L;*/
	
	private Long currentDayUvs = 0L;

	/*private Long previousDayUvs = 0L;
	
	private Long currentDayMUvs = 0L;

	private Long previousDayMUvs = 0L;
	
	private Long currentDayWUvs = 0L;

	private Long previousDayWUvs = 0L;
	
	private Long currentDayAUvs = 0L;

	private Long previousDayAUvs = 0L;
	
	private Long currentDayIUvs = 0L;

	private Long previousDayIUvs = 0L;

	private Long currentDayMSlideDepth;

	private Long previousDayMSlideDepth;
	
	private Long currentDaySlideDepth;

	private Long previousDaySlideDepth;

	private Long currentDayWSlideDepth;

	private Long previousDayWSlideDepth;
	
	private Long currentDayASlideDepth;

	private Long previousDayASlideDepth;
	
	private Long currentDayISlideDepth;

	private Long previousDayISlideDepth;

	private Long currentDaySocialTraffic = 0L;

	private Long previousDaySocialTraffic = 0L;

	private Long currentDayStoryCount;

	private Long previousDayStoryCount;*/
	
	private Long shares = 0L;
	
	private Long sessions = 0L;
	
	private Double shareability = 0.0;
	
	private List<StoryDetail> stories;

	public Long getCurrentDayPvs() {
		return currentDayPvs;
	}

	public void setCurrentDayPvs(Long currentDayPvs) {
		this.currentDayPvs = currentDayPvs;
	}

	/*public Long getPreviousDayPvs() {
		return previousDayPvs;
	}

	public void setPreviousDayPvs(Long previousDayPvs) {
		this.previousDayPvs = previousDayPvs;
	}

	public Long getCurrentDayMPvs() {
		return currentDayMPvs;
	}

	public void setCurrentDayMPvs(Long currentDayMPvs) {
		this.currentDayMPvs = currentDayMPvs;
	}

	public Long getPreviousDayMPvs() {
		return previousDayMPvs;
	}

	public void setPreviousDayMPvs(Long previousDayMPvs) {
		this.previousDayMPvs = previousDayMPvs;
	}

	public Long getCurrentDayWPvs() {
		return currentDayWPvs;
	}

	public void setCurrentDayWPvs(Long currentDayWPvs) {
		this.currentDayWPvs = currentDayWPvs;
	}

	public Long getPreviousDayWPvs() {
		return previousDayWPvs;
	}

	public void setPreviousDayWPvs(Long previousDayWPvs) {
		this.previousDayWPvs = previousDayWPvs;
	}

	public Long getCurrentDayMSlideDepth() {
		return currentDayMSlideDepth;
	}

	public void setCurrentDayMSlideDepth(Long currentDayMSlideDepth) {
		this.currentDayMSlideDepth = currentDayMSlideDepth;
	}

	public Long getPreviousDayMSlideDepth() {
		return previousDayMSlideDepth;
	}

	public void setPreviousDayMSlideDepth(Long previousDayMSlideDepth) {
		this.previousDayMSlideDepth = previousDayMSlideDepth;
	}

	public Long getCurrentDaySlideDepth() {
		return currentDaySlideDepth;
	}

	public void setCurrentDaySlideDepth(Long currentDaySlideDepth) {
		this.currentDaySlideDepth = currentDaySlideDepth;
	}

	public Long getPreviousDaySlideDepth() {
		return previousDaySlideDepth;
	}

	public void setPreviousDaySlideDepth(Long previousDaySlideDepth) {
		this.previousDaySlideDepth = previousDaySlideDepth;
	}

	public Long getCurrentDayWSlideDepth() {
		return currentDayWSlideDepth;
	}

	public void setCurrentDayWSlideDepth(Long currentDayWSlideDepth) {
		this.currentDayWSlideDepth = currentDayWSlideDepth;
	}

	public Long getPreviousDayWSlideDepth() {
		return previousDayWSlideDepth;
	}

	public void setPreviousDayWSlideDepth(Long previousDayWSlideDepth) {
		this.previousDayWSlideDepth = previousDayWSlideDepth;
	}

	public Long getCurrentDaySocialTraffic() {
		return currentDaySocialTraffic;
	}

	public void setCurrentDaySocialTraffic(Long currentDaySocialTraffic) {
		this.currentDaySocialTraffic = currentDaySocialTraffic;
	}

	public Long getPreviousDaySocialTraffic() {
		return previousDaySocialTraffic;
	}

	public void setPreviousDaySocialTraffic(Long previousDaySocialTraffic) {
		this.previousDaySocialTraffic = previousDaySocialTraffic;
	}

	public Long getCurrentDayStoryCount() {
		return currentDayStoryCount;
	}

	public void setCurrentDayStoryCount(Long currentDayStoryCount) {
		this.currentDayStoryCount = currentDayStoryCount;
	}

	public Long getPreviousDayStoryCount() {
		return previousDayStoryCount;
	}

	public void setPreviousDayStoryCount(Long previousDayStoryCount) {
		this.previousDayStoryCount = previousDayStoryCount;
	}*/

	public List<StoryDetail> getStories() {
		return stories;
	}

	public void setStories(List<StoryDetail> stories) {
		this.stories = stories;
	}	

	/*public Long getCurrentDayAPvs() {
		return currentDayAPvs;
	}

	public void setCurrentDayAPvs(Long currentDayAPvs) {
		this.currentDayAPvs = currentDayAPvs;
	}

	public Long getPreviousDayAPvs() {
		return previousDayAPvs;
	}

	public void setPreviousDayAPvs(Long previousDayAPvs) {
		this.previousDayAPvs = previousDayAPvs;
	}

	public Long getCurrentDayIPvs() {
		return currentDayIPvs;
	}

	public void setCurrentDayIPvs(Long currentDayIPvs) {
		this.currentDayIPvs = currentDayIPvs;
	}

	public Long getPreviousDayIPvs() {
		return previousDayIPvs;
	}

	public void setPreviousDayIPvs(Long previousDayIPvs) {
		this.previousDayIPvs = previousDayIPvs;
	}

	public Long getCurrentDayASlideDepth() {
		return currentDayASlideDepth;
	}

	public void setCurrentDayASlideDepth(Long currentDayASlideDepth) {
		this.currentDayASlideDepth = currentDayASlideDepth;
	}

	public Long getPreviousDayASlideDepth() {
		return previousDayASlideDepth;
	}

	public void setPreviousDayASlideDepth(Long previousDayASlideDepth) {
		this.previousDayASlideDepth = previousDayASlideDepth;
	}

	public Long getCurrentDayISlideDepth() {
		return currentDayISlideDepth;
	}

	public void setCurrentDayISlideDepth(Long currentDayISlideDepth) {
		this.currentDayISlideDepth = currentDayISlideDepth;
	}

	public Long getPreviousDayISlideDepth() {
		return previousDayISlideDepth;
	}

	public void setPreviousDayISlideDepth(Long previousDayISlideDepth) {
		this.previousDayISlideDepth = previousDayISlideDepth;
	}
*/
	public Long getCurrentDayUvs() {
		return currentDayUvs;
	}

	public void setCurrentDayUvs(Long currentDayUvs) {
		this.currentDayUvs = currentDayUvs;
	}
	/*
	public Long getPreviousDayUvs() {
		return previousDayUvs;
	}

	public void setPreviousDayUvs(Long previousDayUvs) {
		this.previousDayUvs = previousDayUvs;
	}

	public Long getCurrentDayMUvs() {
		return currentDayMUvs;
	}

	public void setCurrentDayMUvs(Long currentDayMUvs) {
		this.currentDayMUvs = currentDayMUvs;
	}

	public Long getPreviousDayMUvs() {
		return previousDayMUvs;
	}

	public void setPreviousDayMUvs(Long previousDayMUvs) {
		this.previousDayMUvs = previousDayMUvs;
	}

	public Long getCurrentDayWUvs() {
		return currentDayWUvs;
	}

	public void setCurrentDayWUvs(Long currentDayWUvs) {
		this.currentDayWUvs = currentDayWUvs;
	}

	public Long getPreviousDayWUvs() {
		return previousDayWUvs;
	}

	public void setPreviousDayWUvs(Long previousDayWUvs) {
		this.previousDayWUvs = previousDayWUvs;
	}

	public Long getCurrentDayAUvs() {
		return currentDayAUvs;
	}

	public void setCurrentDayAUvs(Long currentDayAUvs) {
		this.currentDayAUvs = currentDayAUvs;
	}

	public Long getPreviousDayAUvs() {
		return previousDayAUvs;
	}

	public void setPreviousDayAUvs(Long previousDayAUvs) {
		this.previousDayAUvs = previousDayAUvs;
	}

	public Long getCurrentDayIUvs() {
		return currentDayIUvs;
	}

	public void setCurrentDayIUvs(Long currentDayIUvs) {
		this.currentDayIUvs = currentDayIUvs;
	}

	public Long getPreviousDayIUvs() {
		return previousDayIUvs;
	}

	public void setPreviousDayIUvs(Long previousDayIUvs) {
		this.previousDayIUvs = previousDayIUvs;
	}
*/
	public Long getShares() {
		return shares;
	}

	public void setShares(Long shares) {
		this.shares = shares;
	}

	public Double getShareability() {
		return shareability;
	}

	public void setShareability(Double shareability) {
		this.shareability = shareability;
	}
	
	public Long getSessions() {
		return sessions;
	}

	public void setSessions(Long sessions) {
		this.sessions = sessions;
	}

	
	

	/*+ ", previousDayPvs=" + previousDayPvs + ", currentDayMPvs="
	+ currentDayMPvs + ", previousDayMPvs=" + previousDayMPvs + ", currentDayWPvs=" + currentDayWPvs
	+ ", previousDayWPvs=" + previousDayWPvs + ", currentDayMSlideDepth=" + currentDayMSlideDepth
	+ ", previousDayMSlideDepth=" + previousDayMSlideDepth + ", currentDaySlideDepth="
	+ currentDaySlideDepth + ", previousDaySlideDepth=" + previousDaySlideDepth + ", currentDayWSlideDepth="
	+ currentDayWSlideDepth + ", previousDayWSlideDepth=" + previousDayWSlideDepth
	+ ", currentDaySocialTraffic=" + currentDaySocialTraffic + ", previousDaySocialTraffic="
	+ previousDaySocialTraffic + ", currentDayStoryCount=" + currentDayStoryCount
	+ ", previousDayStoryCount=" + previousDayStoryCount*/
	
	
	@Override
	public String toString() {
		return "WisdomPage [currentDayPvs=" + currentDayPvs  + ", stories=" + stories +", shareability=" + shareability +", shares=" + shares + ", sessions=" + sessions +"]";
	}

}
