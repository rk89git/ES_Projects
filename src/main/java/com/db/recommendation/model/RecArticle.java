package com.db.recommendation.model;


/**
 * Model Class to return Recommendation result.
 */
public class RecArticle {

	public String title;
	
	public String url;

	public String relative_url=null;
	
	public String story_id;

	public String brand_id;

	public String brand_url;

	public String brand_name;

	public String description;

	public String image;

	public String keywords;

	public String news_keywords;

	public String language;

	public String published_date;

	public String section;

	public String views;

	/**
	 * @return the title
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * @param title the title to set
	 */
	public void setTitle(String title) {
		this.title = title;
	}

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
	}

	/**
	 * @return the relative_url
	 */
	public String getRelative_url() {
		return relative_url;
	}

	/**
	 * @param relative_url the relative_url to set
	 */
	public void setRelative_url(String relative_url) {
		this.relative_url = relative_url;
	}

	
	/**
	 * @return the story_id
	 */
	public String getStory_id() {
		return story_id;
	}

	/**
	 * @param story_id the story_id to set
	 */
	public void setStory_id(String story_id) {
		this.story_id = story_id;
	}

	/**
	 * @return the brand_id
	 */
	public String getBrand_id() {
		return brand_id;
	}

	/**
	 * @param brand_id the brand_id to set
	 */
	public void setBrand_id(String brand_id) {
		this.brand_id = brand_id;
	}

	/**
	 * @return the brand_url
	 */
	public String getBrand_url() {
		return brand_url;
	}

	/**
	 * @param brand_url the brand_url to set
	 */
	public void setBrand_url(String brand_url) {
		this.brand_url = brand_url;
	}

	/**
	 * @return the brand_name
	 */
	public String getBrand_name() {
		return brand_name;
	}

	/**
	 * @param brand_name the brand_name to set
	 */
	public void setBrand_name(String brand_name) {
		this.brand_name = brand_name;
	}

	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return the image
	 */
	public String getImage() {
		return image;
	}

	/**
	 * @param image the image to set
	 */
	public void setImage(String image) {
		this.image = image;
	}

	/**
	 * @return the keywords
	 */
	public String getKeywords() {
		return keywords;
	}

	/**
	 * @param keywords the keywords to set
	 */
	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}

	/**
	 * @return the news_keywords
	 */
	public String getNews_keywords() {
		return news_keywords;
	}

	/**
	 * @param news_keywords the news_keywords to set
	 */
	public void setNews_keywords(String news_keywords) {
		this.news_keywords = news_keywords;
	}

	/**
	 * @return the language
	 */
	public String getLanguage() {
		return language;
	}

	/**
	 * @param language the language to set
	 */
	public void setLanguage(String language) {
		this.language = language;
	}
	
	/**
	 * @return the published_date
	 */
	public String getPublishedDate() {
		return published_date;
	}

	/**
	 * @param published_date the published_date to set
	 */
	public void setPublishedDate(String published_date) {
		this.published_date = published_date;
	}

	/**
	 * @return the section
	 */
	public String getSection() {
		return section;
	}

	/**
	 * @param section the section to set
	 */
	public void setSection(String section) {
		this.section = section;
	}

	/**
	 * @return the views
	 */
	public String getViews() {
		return views;
	}

	/**
	 * @param views the views to set
	 */
	public void setViews(String views) {
		this.views = views;
	}

	@Override
	  public String toString() {
	    return "\ntitle:"+title+"\n keywords: "+ keywords+"\n views: "+views+" \nstoryId: "+story_id+"\n brand_id : "+brand_id +" brand_name: "+brand_name +"image: "+image+" url: "+url +" brand_url: "+brand_url;
	  }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((story_id == null) ? 0 : story_id.hashCode());
		result = prime * result + ((title == null) ? 0 : title.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RecArticle other = (RecArticle) obj;
		if (story_id == null) {
			if (other.story_id != null)
				return false;
		} else if (!story_id.equals(other.story_id))
			return false;
		if (title == null) {
			if (other.title != null)
				return false;
		} else if (!title.equals(other.title))
			return false;
		return true;
	}
	
	

	/**
	 * 
	 * 
	 * title url story_id brand_id "brand_url"
	 * 
	 * "brand_name" "referal_domain"
	 * 
	 * "datetime" "description"
	 * 
	 * "image" "keywords" "meta_nouns" entities "news_keywords" "language"
	 * "continent" "country" "state" "city" "published_date" "section"
	 * "device_type" "views"
	 * 
	 */

}
