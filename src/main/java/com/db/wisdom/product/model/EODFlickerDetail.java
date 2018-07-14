package com.db.wisdom.product.model;

import java.util.List;

public class EODFlickerDetail {

	private String storyid;
	
	private String title;
	
	private String url;
	
	private String startDate;
	
	private String endDate;
	
	private Integer cat_id;
	
	private Long totalpvs;
	
	private Long mpvs;
	
	private Long wpvs;
	
    private Long totaluvs;
	
	private Long muvs;
	
	private Long wuvs;
	
	private Long minutespvs;
	
	private Long minutesuvs;
	
	private Integer position;
	
	private Integer recPosition;
	
	private List<Integer> prevPositions;
	
	private Boolean isAvailable = false;

	public Boolean getIsAvailable() {
		return isAvailable;
	}
	public void setIsAvailable(Boolean isAvailable) {
		this.isAvailable = isAvailable;
	}
	public String getStoryid() {
		return storyid;
	}
	public void setStoryid(String storyid) {
		this.storyid = storyid;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getStartDate() {
		return startDate;
	}
	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}
	public String getEndDate() {
		return endDate;
	}
	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}
	public Long getTotalpvs() {
		return totalpvs;
	}
	public void setTotalpvs(Long totalpvs) {
		this.totalpvs = totalpvs;
	}
	public Long getMpvs() {
		return mpvs;
	}
	public void setMpvs(Long mpvs) {
		this.mpvs = mpvs;
	}
	public Long getWpvs() {
		return wpvs;
	}
	public void setWpvs(Long wpvs) {
		this.wpvs = wpvs;
	}
	public Long getMinutespvs() {
		return minutespvs;
	}
	public void setMinutespvs(Long minutespvs) {
		this.minutespvs = minutespvs;
	}
	public Integer getPosition() {
		return position;
	}
	public void setPosition(Integer position) {
		this.position = position;
	}
	public Integer getRecPosition() {
		return recPosition;
	}
	public void setRecPosition(Integer recPosition) {
		this.recPosition = recPosition;
	}
	public List<Integer> getPrevPositions() {
		return prevPositions;
	}
	public void setPrevPositions(List<Integer> prevPositions) {
		this.prevPositions = prevPositions;
	}
	public Integer getCat_id() {
		return cat_id;
	}
	public void setCat_id(Integer cat_id) {
		this.cat_id = cat_id;
	}
	public Long getTotaluvs() {
		return totaluvs;
	}
	public void setTotaluvs(Long totaluvs) {
		this.totaluvs = totaluvs;
	}
	public Long getMuvs() {
		return muvs;
	}
	public void setMuvs(Long muvs) {
		this.muvs = muvs;
	}
	public Long getWuvs() {
		return wuvs;
	}
	public void setWuvs(Long wuvs) {
		this.wuvs = wuvs;
	}
	public Long getMinutesuvs() {
		return minutesuvs;
	}
	public void setMinutesuvs(Long minutesuvs) {
		this.minutesuvs = minutesuvs;
	}	
	
}
