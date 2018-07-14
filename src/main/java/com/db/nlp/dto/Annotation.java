package com.db.nlp.dto;

import org.codehaus.jackson.annotate.JsonAnyGetter;
import org.codehaus.jackson.annotate.JsonGetter;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonValue;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.annotate.JsonView;
import scala.collection.script.Include;

import java.io.*;
import java.util.List;

/**
 * Created by DB on 15-05-2017.
 */

public class Annotation  implements Serializable{

    private int begin;
    private int end;

    transient private String documentText;
    private String coveredText;

    public void setCoveredText(String coveredText) {
        this.coveredText = coveredText;
    }

    public String getCoveredText() {
      return this.documentText.substring(begin,end);
    }



    public int getBegin() {
        return begin;
    }

    public void setBegin(int begin) {
        this.begin = begin;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public String getDocumentText() {
        return documentText.substring(begin,end);
    }

    public void setDocumentText(String documentText) {
        this.documentText = documentText;
    }


    private void writeObject(ObjectOutputStream oos)
            throws IOException {
      oos.defaultWriteObject();
      oos.writeObject(getCoveredText());
        System.out.println(
               "serialize method called"
        );
    }

    private void readObject(ObjectInputStream ois)
            throws ClassNotFoundException, IOException {


ois.defaultReadObject();
        coveredText=(String)ois.readObject();
        System.out.println(
                "deserialize method called"
        );


    }

}
