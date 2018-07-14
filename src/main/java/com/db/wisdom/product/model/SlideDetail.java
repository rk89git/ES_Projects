package com.db.wisdom.product.model;

public class SlideDetail {

	private Integer slideNo;

	private Long mpvs = 0L;
	
	private Long wpvs = 0L;
	
	private Long apvs = 0L;
	
	private Long ipvs = 0L;

	public Integer getSlideNo() {
		return slideNo;
	}

	public void setSlideNo(Integer slideNo) {
		this.slideNo = slideNo;
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

	public Long getApvs() {
		return apvs;
	}

	public void setApvs(Long apvs) {
		this.apvs = apvs;
	}

	public Long getIpvs() {
		return ipvs;
	}

	public void setIpvs(Long ipvs) {
		this.ipvs = ipvs;
	}
	

}
