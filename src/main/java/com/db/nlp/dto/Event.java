package com.db.nlp.dto;

/**
 * Created by DB on 15-05-2017.
 */
public class Event extends Annotation {

    Annotation annotation;

    public Event(Annotation annotation) {
        this.annotation = annotation;
        this.setDocumentText(annotation.getDocumentText());

    }
}
