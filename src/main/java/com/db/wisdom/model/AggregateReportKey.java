package com.db.wisdom.model;

/**
 * Created by Satya on 15-03-2018.
 */
public class AggregateReportKey {

    private String channelNumber;
    private String host;
    private String identifierValue;
    private String identifierType;
    private String date;

    public AggregateReportKey(String channelNumber, String host, String identifierValue, String identifierType, String date) {
        this.channelNumber = channelNumber;
        this.host = host;
        this.identifierValue = identifierValue;
        this.identifierType = identifierType;
        this.date = date;
    }

    public String getChannelNumber() {
        return channelNumber;
    }

    public void setChannelNumber(String channelNumber) {
        this.channelNumber = channelNumber;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getIdentifierValue() {
        return identifierValue;
    }

    public void setIdentifierValue(String identifierValue) {
        this.identifierValue = identifierValue;
    }

    public String getIdentifierType() {
        return identifierType;
    }

    public void setIdentifierType(String identifierType) {
        this.identifierType = identifierType;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AggregateReportKey that = (AggregateReportKey) o;

        if (channelNumber != null ? !channelNumber.equals(that.channelNumber) : that.channelNumber != null)
            return false;
        if (host != null ? !host.equals(that.host) : that.host != null) return false;
        if (identifierValue != null ? !identifierValue.equals(that.identifierValue) : that.identifierValue != null)
            return false;
        if (identifierType != null ? !identifierType.equals(that.identifierType) : that.identifierType != null)
            return false;
        return date != null ? date.equals(that.date) : that.date == null;
    }

    @Override
    public int hashCode() {
        int result = channelNumber != null ? channelNumber.hashCode() : 0;
        result = 31 * result + (host != null ? host.hashCode() : 0);
        result = 31 * result + (identifierValue != null ? identifierValue.hashCode() : 0);
        result = 31 * result + (identifierType != null ? identifierType.hashCode() : 0);
        result = 31 * result + (date != null ? date.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AggregateReportKey{" +
                "channelNumber='" + channelNumber + '\'' +
                ", host='" + host + '\'' +
                ", identifierValue='" + identifierValue + '\'' +
                ", identifierType='" + identifierType + '\'' +
                ", date='" + date + '\'' +
                '}';
    }


}
