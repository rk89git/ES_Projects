package com.db.recommendation.model;

/**
 * Model Class to receive queries of Key Recommendation event.
 */
public class RecQuery {

	/** The story id. */
	private String story_id;
	
	/** The session id. */
	private String session_id;
	
	/** The brand url. */
	private String brand_id;
	
	/** The size */
	private int size = 15;
	
	private int storyCountQualifier = 5;
	
	
	/**
	 * Gets the session id .
	 *
	 * @return the session id
	 */
	public String getSessionId() {
		return session_id;
	}

	/**
	 * Sets the session id.
	 *
	 * @param session_id the session id
	 */
	public void setSessionId(String session_id) {
		this.session_id = session_id;
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
	 * Gets the size .
	 *
	 * @return the size
	 */
	public int getSize() {
		return size;
	}

	/**
	 * Sets the size.
	 *
	 * @param size the size
	 */
	public void setSize(int size) {
		this.size = size;
	}

	public String getStory_id() {
		return story_id;
	}

	public void setStory_id(String story_id) {
		this.story_id = story_id;
	}

	public String getSession_id() {
		return session_id;
	}

	public void setSession_id(String session_id) {
		this.session_id = session_id;
	}

	public int getStoryCountQualifier() {
		return storyCountQualifier;
	}

	public void setStoryCountQualifier(int storyCountQualifier) {
		this.storyCountQualifier = storyCountQualifier;
	}
	
}
