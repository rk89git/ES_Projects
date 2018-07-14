package com.db.wisdom.model;

/**
 * Created by Satya on 14-03-2018.
 */
public class AggregateReportValue {

    private double uniqueVisitors;
    private double uniqueVisitorsMTD;
    private double uniqueVisitorsPrev30Days;
    private double uniquePageViews;
    private double uniquePageViewsMTD;
    private double uniquePageViewsPrev30Days;
    private double pageViews;
    private double pageViewsMTD;
    private double pageViewsPrev30Days;
    private double sessions;
    private double sessionsMTD;
    private double sessionsPrev30Days;

    public double getUniqueVisitors() {
        return uniqueVisitors;
    }

    public void setUniqueVisitors(double uniqueVisitors) {
        this.uniqueVisitors = uniqueVisitors;
    }

    public double getUniqueVisitorsMTD() {
        return uniqueVisitorsMTD;
    }

    public void setUniqueVisitorsMTD(double uniqueVisitorsMTD) {
        this.uniqueVisitorsMTD = uniqueVisitorsMTD;
    }

    public double getUniquePageViews() {
        return uniquePageViews;
    }

    public void setUniquePageViews(double uniquePageViews) {
        this.uniquePageViews = uniquePageViews;
    }

    public double getUniquePageViewsMTD() {
        return uniquePageViewsMTD;
    }

    public void setUniquePageViewsMTD(double uniquePageViewsMTD) {
        this.uniquePageViewsMTD = uniquePageViewsMTD;
    }

    public double getPageViews() {
        return pageViews;
    }

    public void setPageViews(double pageViews) {
        this.pageViews = pageViews;
    }

    public double getPageViewsMTD() {
        return pageViewsMTD;
    }

    public void setPageViewsMTD(double pageViewsMTD) {
        this.pageViewsMTD = pageViewsMTD;
    }

    public double getSessions() {
        return sessions;
    }

    public void setSessions(double sessions) {
        this.sessions = sessions;
    }

    public double getSessionsMTD() {
        return sessionsMTD;
    }

    public void setSessionsMTD(double sessionsMTD) {
        this.sessionsMTD = sessionsMTD;
    }    

    public double getUniqueVisitorsPrev30Days() {
		return uniqueVisitorsPrev30Days;
	}

	public void setUniqueVisitorsPrev30Days(double uniqueVisitorsPrev30Days) {
		this.uniqueVisitorsPrev30Days = uniqueVisitorsPrev30Days;
	}

	public double getUniquePageViewsPrev30Days() {
		return uniquePageViewsPrev30Days;
	}

	public void setUniquePageViewsPrev30Days(double uniquePageViewsPrev30Days) {
		this.uniquePageViewsPrev30Days = uniquePageViewsPrev30Days;
	}

	public double getPageViewsPrev30Days() {
		return pageViewsPrev30Days;
	}

	public void setPageViewsPrev30Days(double pageViewsPrev30Days) {
		this.pageViewsPrev30Days = pageViewsPrev30Days;
	}

	public double getSessionsPrev30Days() {
		return sessionsPrev30Days;
	}

	public void setSessionsPrev30Days(double sessionsPrev30Days) {
		this.sessionsPrev30Days = sessionsPrev30Days;
	}

	@Override
    public String toString() {
        return "AggregateReportValue{" +
                "uniqueVisitors=" + uniqueVisitors +
                ", uniqueVisitorsMTD=" + uniqueVisitorsMTD +
                ",uniqueVisitorsPrev30Days="+ uniqueVisitorsPrev30Days+
                ", uniquePageViews=" + uniquePageViews +
                ", uniquePageViewsMTD=" + uniquePageViewsMTD +
                ", uniquePageViewsPrev30Days=" + uniquePageViewsPrev30Days+
                ", pageViews=" + pageViews +
                ", pageViewsMTD=" + pageViewsMTD +
                ", pageViewsPrev30Days=" + pageViewsPrev30Days +
                ", sessions=" + sessions +
                ", sessionsMTD=" + sessionsMTD +
                ", sessionsPrev30Days=" + sessionsPrev30Days +
                '}';
    }
}
