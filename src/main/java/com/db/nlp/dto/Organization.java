package com.db.nlp.dto;

/**
 * Created by DB on 15-05-2017.
 */
public class Organization extends Annotation {

    Annotation annotation;

    public Organization(Annotation annotation) {
        this.annotation = annotation;
        this.setDocumentText(annotation.getDocumentText());
    }
}
