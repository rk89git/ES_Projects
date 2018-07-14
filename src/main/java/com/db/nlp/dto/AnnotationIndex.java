package com.db.nlp.dto;

import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DB on 15-05-2017.
 */
@Component
public class AnnotationIndex {


    List<People> peopleList = new ArrayList<>();
    List<Location> locationList = new ArrayList<>();
    List<Organization> organizationList = new ArrayList<>();
    List<Event> eventList = new ArrayList<>();


  public  void addToIndex(Annotation annotation) {
        if (annotation instanceof People)
            peopleList.add((People) annotation);
        if (annotation instanceof Organization)
            organizationList.add((Organization) annotation);
        if (annotation instanceof Location)
            locationList.add((Location) annotation);
        if (annotation instanceof Event)
            eventList.add((Event) annotation);

    }

 public  List<? extends Annotation>  getEntityList(Class clazz)
    {
        if(clazz==People.class)
            return peopleList;
        if(clazz==Location.class)
            return locationList;
        if(clazz==Event.class)
            return eventList;
        if(clazz==Organization.class)
            return organizationList;

        return null;

    }




}
