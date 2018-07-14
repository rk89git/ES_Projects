package com.db.nlp.dto;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by DB on 15-05-2017.
 */
public class EntityTextDTO {

   static public EntityTextDTO getEmptyDTO()
    {
        return  new EntityTextDTO();
    }

   private Set<String> peopleList= new HashSet<>();
    private Set<String> LocationList= new HashSet<>();
    private Set<String> organizationList= new HashSet<>();
    private Set<String> eventsList= new HashSet<>();

    public Set<String> getPeopleList() {
        return peopleList;
    }

    public void setPeopleList(Set<String> peopleList) {
        this.peopleList = peopleList;
    }

    public Set<String> getLocationList() {
        return LocationList;
    }

    public void setLocationList(Set<String> locationList) {
        LocationList = locationList;
    }

    public Set<String> getOrganizationList() {
        return organizationList;
    }

    public void setOrganizationList(Set<String> organizationList) {
        this.organizationList = organizationList;
    }

    public Set<String> getEventsList() {
        return eventsList;
    }

    public void setEventsList(Set<String> eventsList) {
        this.eventsList = eventsList;
    }
}
