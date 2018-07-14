package com.db.nlp.dto;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DB on 15-05-2017.
 */
public class EntityDTO {

   static public  EntityDTO getEmptyDTO()
    {
        return  new EntityDTO();
    }

   private List<? extends  Annotation> peopleList= new ArrayList<>();
    private List<? extends Annotation> LocationList= new ArrayList<>();
    private List< ? extends Annotation> organizationList= new ArrayList<>();
    private List< ? extends Annotation> eventsList= new ArrayList<>();

    public List<? extends Annotation> getPeopleList() {
        return peopleList;
    }

    public void setPeopleList(List<? extends Annotation> peopleList) {
        this.peopleList = peopleList;
    }

    public List<? extends Annotation> getLocationList() {
        return LocationList;
    }

    public void setLocationList(List<? extends Annotation> locationList) {
        LocationList = locationList;
    }

    public List<? extends Annotation> getOrganizationList() {
        return organizationList;
    }

    public void setOrganizationList(List<? extends Annotation> organizationList) {
        this.organizationList = organizationList;
    }

    public List<? extends Annotation> getEventsList() {
        return eventsList;
    }

    public void setEventsList(List<? extends Annotation> eventsList) {
        this.eventsList = eventsList;
    }

    @Override
    public String toString() {
        return "EntityDTO{" +
                "peopleList=" + peopleList +
                ", LocationList=" + LocationList +
                ", organizationList=" + organizationList +
                ", locationList=" + eventsList +
                '}';
    }
}
