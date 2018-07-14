package com.db.wisdom.product.model;

import java.util.List;
import java.util.Map;

public class StoryDetail {

	private String author;

	private String article_image;

	private Boolean isUCB;

	private Boolean isFacebook;

	private Long mpvs;
	
	private Long mupvs;
	
	private Long wupvs;

	private Long mSlideDepth;
	
	private Long wSlideDepth;
	
	private Long aSlideDepth;
	
	private Long iSlideDepth;

	private Long slideDepth;
	
	private Long pvContribution;
	
	private Long uvContribution;

	private Map<String, Map<String, Long>> pageViewsTracker;// = new HashMap<>();

	private Integer slides;

	private Map<String, Map<String, Long>> sourceWiseTraffic;// = new HashMap<>();

	private String story_id;

	private String published_date;

	private String title;

	private Long totalpvs;

	private Long totalupvs;

	private String author_id;

	private String url;

	private String modified_date;
	
	private String domain_id;

	private Long wpvs;
	
	private Long apvs;
	
	private Long ipvs;
	
	private Long aupvs;
	
	private Long iupvs;
	
	private WisdomArticleRating rating;
	
	private List<String> feedback;
	
	private String bestHour;
	
	private String bestDay;
	
	private String maxRange;
	
	private String maxProb;
	
	private String bestNearestHour;
	
	private String bestNearestDay;
	
	private String bestNearestHourrange;
	
	private String bestNearestHourProb;
	
	private Long commentCount;
	
	private Long pvs;
	
	public WisdomArticleRating getRating() {
		return rating;
	}

	public void setRating(WisdomArticleRating rating) {
		this.rating = rating;
	}

	public List<String> getFeedback() {
		return feedback;
	}

	public void setFeedback(List<String> storyfeedback) {
		this.feedback = storyfeedback;
	}


	
	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public String getArticle_image() {
		return article_image;
	}

	public void setArticle_image(String article_image) {
		this.article_image = article_image;
	}

	public Boolean getIsUCB() {
		return isUCB;
	}

	public void setIsUCB(Boolean isUCB) {
		this.isUCB = isUCB;
	}

	public Boolean getIsFacebook() {
		return isFacebook;
	}

	public void setIsFacebook(Boolean isFacebook) {
		this.isFacebook = isFacebook;
	}

	public Long getMpvs() {
		return mpvs;
	}

	public void setMpvs(Long mpvs) {
		this.mpvs = mpvs;
	}

	public Long getmSlideDepth() {
		return mSlideDepth;
	}

	public void setmSlideDepth(Long mSlideDepth) {
		this.mSlideDepth = mSlideDepth;
	}	

	public Long getwSlideDepth() {
		return wSlideDepth;
	}

	public void setwSlideDepth(Long wSlideDepth) {
		this.wSlideDepth = wSlideDepth;
	}

	public Long getSlideDepth() {
		return slideDepth;
	}

	public void setSlideDepth(Long slideDepth) {
		this.slideDepth = slideDepth;
	}
	
	public Long getaSlideDepth() {
		return aSlideDepth;
	}

	public void setaSlideDepth(Long aSlideDepth) {
		this.aSlideDepth = aSlideDepth;
	}

	public Long getiSlideDepth() {
		return iSlideDepth;
	}

	public void setiSlideDepth(Long iSlideDepth) {
		this.iSlideDepth = iSlideDepth;
	}

	public Map<String, Map<String, Long>> getPageViewsTracker() {
		return pageViewsTracker;
	}

	public void setPageViewsTracker(Map<String, Map<String, Long>> pageViewsTracker) {
		this.pageViewsTracker = pageViewsTracker;
	}

	public Integer getSlides() {
		return slides;
	}

	public void setSlides(Integer slides) {
		this.slides = slides;
	}

	public Map<String, Map<String, Long>> getSourceWiseTraffic() {
		return sourceWiseTraffic;
	}

	public void setSourceWiseTraffic(Map<String, Map<String, Long>> sourceWiseTraffic) {
		this.sourceWiseTraffic = sourceWiseTraffic;
	}

	public String getStory_id() {
		return story_id;
	}

	public void setStory_id(String story_id) {
		this.story_id = story_id;
	}

	public String getPublished_date() {
		return published_date;
	}

	public void setPublished_date(String published_date) {
		this.published_date = published_date;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public Long getTotalpvs() {
		return totalpvs;
	}

	public void setTotalpvs(Long totalpvs) {
		this.totalpvs = totalpvs;
	}

	public Long getTotalupvs() {
		return totalupvs;
	}

	public void setTotalupvs(Long totaluvs) {
		this.totalupvs = totaluvs;
	}

	public String getAuthor_id() {
		return author_id;
	}

	public void setAuthor_id(String author_id) {
		this.author_id = author_id;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getModified_date() {
		return modified_date;
	}

	public void setModified_date(String modified_date) {
		this.modified_date = modified_date;
	}

	public Long getWpvs() {
		return wpvs;
	}

	public void setWpvs(Long wpvs) {
		this.wpvs = wpvs;
	}

	public String getDomain_id() {
		return domain_id;
	}

	public void setDomain_id(String domain_id) {
		this.domain_id = domain_id;
	}

	public Long getPvContribution() {
		return pvContribution;
	}

	public void setPvContribution(Long pvContribution) {
		this.pvContribution = pvContribution;
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

	public Long getMupvs() {
		return mupvs;
	}

	public void setMupvs(Long muvs) {
		this.mupvs = muvs;
	}

	public Long getWupvs() {
		return wupvs;
	}

	public void setWupvs(Long wuvs) {
		this.wupvs = wuvs;
	}

	public Long getAupvs() {
		return aupvs;
	}

	public void setAupvs(Long auvs) {
		this.aupvs = auvs;
	}

	public Long getIupvs() {
		return iupvs;
	}

	public void setIupvs(Long iuvs) {
		this.iupvs = iuvs;
	}

	public Long getUvContribution() {
		return uvContribution;
	}

	public void setUvContribution(Long uvContribution) {
		this.uvContribution = uvContribution;
	}

	public String getBestHour() {
		return bestHour;
	}

	public void setBestHour(String bestHour) {
		this.bestHour = bestHour;
	}

	public String getBestDay() {
		return bestDay;
	}

	public void setBestDay(String bestDay) {
		this.bestDay = bestDay;
	}

	public String getMaxRange() {
		return maxRange;
	}

	public void setMaxRange(String maxRange) {
		this.maxRange = maxRange;
	}

	public String getMaxProb() {
		return maxProb;
	}

	public void setMaxProb(String maxProb) {
		this.maxProb = maxProb;
	}

	public String getBestNearestHour() {
		return bestNearestHour;
	}

	public void setBestNearestHour(String bestNearestHour) {
		this.bestNearestHour = bestNearestHour;
	}

	public String getBestNearestDay() {
		return bestNearestDay;
	}

	public void setBestNearestDay(String bestNearestDay) {
		this.bestNearestDay = bestNearestDay;
	}

	public String getBestNearestHourrange() {
		return bestNearestHourrange;
	}

	public void setBestNearestHourrange(String bestNearestHourrange) {
		this.bestNearestHourrange = bestNearestHourrange;
	}

	public String getBestNearestHourProb() {
		return bestNearestHourProb;
	}

	public void setBestNearestHourProb(String bestNearestHourProb) {
		this.bestNearestHourProb = bestNearestHourProb;
	}

	public Long getCommentCount() {
		return commentCount;
	}

	public void setCommentCount(Long commentCount) {
		this.commentCount = commentCount;
	}

	public Long getPvs() {
		return pvs;
	}

	public void setPvs(Long pvs) {
		this.pvs = pvs;
	}
	
}
