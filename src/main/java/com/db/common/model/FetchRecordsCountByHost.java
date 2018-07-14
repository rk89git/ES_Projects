package com.db.common.model;

import java.util.List;
public class FetchRecordsCountByHost  {

	private String dateValue;
	private List<Integer> pageNumber;
	private String trackerValue;
	private Integer host;
	
	
	
	/**
	 * @return the dateValue
	 */
	public String getDateValue() {
		return dateValue;
	}
	/**
	 * @param dateValue the dateValue to set
	 */
	public void setDateValue(String dateValue) {
		this.dateValue = dateValue;
	}
	/**
	 * @return the pageNumber
	 */
	public List<Integer> getPageNumber() {
		return pageNumber;
	}
	/**
	 * @param pageNumber the pageNumber to set
	 */
	public void setPageNumber(List<Integer> pageNumber) {
		this.pageNumber = pageNumber;
	}
	/**
	 * @return the trackerValue
	 */
	public String getTrackerValue() {
		return trackerValue;
	}
	/**
	 * @param trackerValue the trackerValue to set
	 */
	public void setTrackerValue(String trackerValue) {
		this.trackerValue = trackerValue;
	}
	/**
	 * @return the host
	 */
	public Integer getHost() {
		return host;
	}
	/**
	 * @param host the host to set
	 */
	public void setHost(Integer host) {
		this.host = host;
	}
	
}
