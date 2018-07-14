package com.db.nlp.services;

import com.db.nlp.co.NLPCO;
import com.db.nlp.dto.*;
import com.db.nlp.enums.EntityType;

import org.aspectj.weaver.ast.Or;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;


/**
 * Created by DB on 13-05-2017.
 */
@Service
public class NLPServiceHandler {
    //@Autowired
    //AnnotationIndex annotationIndex;

  /*  AbstractSequenceClassifier<CoreLabel> classifier = null;//intitializeClassifier();

    AbstractSequenceClassifier<CoreLabel> intitializeClassifier() {
        AbstractSequenceClassifier<CoreLabel> crfClassifier = null;
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            crfClassifier = CRFClassifier.getClassifier(classLoader.getResource("classifiers/english.all.3class.distsim.crf.ser.gz").getPath());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return crfClassifier;
    }*/


    public EntityTextDTO getNLPTags(NLPCO nlpco) {/*
        AnnotationIndex annotationIndex = new AnnotationIndex();
        if (nlpco.getContent() == null || nlpco.getContent().isEmpty())
            return EntityTextDTO.getEmptyDTO();
        Annotation annotation = new Annotation();
        annotation.setDocumentText(nlpco.getContent());
        annotation.setBegin(0);
        annotation.setEnd(nlpco.getContent().length() - 1);
        List<Triple<String, Integer, Integer>> triples = classifier.classifyToCharacterOffsets(nlpco.getContent());
        for (Triple<String, Integer, Integer> trip : triples) {
            EntityType entityType = EntityType.getEnumConstant(trip.first());
            if (entityType == EntityType.ENTITY_TYPE_PERSON) {
                People people = new People(annotation);
                people.setBegin(trip.second());
                people.setEnd(trip.third);
                annotationIndex.addToIndex(people);
            }
            if (entityType == EntityType.ENTITY_TYPE_LOCATION) {
                Location location = new Location(annotation);
                location.setBegin(trip.second());
                location.setEnd(trip.third);
                annotationIndex.addToIndex(location);
            }
            if (entityType == EntityType.ENTITY_TYPE_ORGANIZATION) {
                Organization organization = new Organization(annotation);
                organization.setBegin(trip.second());
                organization.setEnd(trip.third);
                annotationIndex.addToIndex(organization);
            }
            if (entityType == EntityType.ENTITY_TYPE_EVENT) {
                Event event = new Event(annotation);
                event.setBegin(trip.second());
                event.setEnd(trip.third);
                annotationIndex.addToIndex(event);
            }

        }

        return prepareEntityTextDTO(annotationIndex);*/
        return null;


    }

    private EntityDTO prepareDTO(AnnotationIndex annotationIndex) {
        System.out.println(annotationIndex);
        EntityDTO dto = new EntityDTO();
        dto.setPeopleList(annotationIndex.getEntityList(People.class));
        dto.setOrganizationList(annotationIndex.getEntityList(Organization.class));
        dto.setEventsList(annotationIndex.getEntityList(Event.class));
        dto.setLocationList(annotationIndex.getEntityList(Location.class));

        return dto;
    }

    private EntityTextDTO prepareEntityTextDTO(AnnotationIndex annotationIndex) {
        EntityTextDTO dto = EntityTextDTO.getEmptyDTO();
        populateWithText(dto.getPeopleList(), annotationIndex.getEntityList(People.class));
        populateWithText(dto.getLocationList(), annotationIndex.getEntityList(Location.class));
        populateWithText(dto.getOrganizationList(), annotationIndex.getEntityList(Organization.class));


        return dto;
    }

    void populateWithText(Set<String> set, List<? extends Annotation> list) {
        for (Annotation annotation : list) {
            set.add(annotation.getCoveredText());
        }
    }


}
