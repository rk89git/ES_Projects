package com.db.nlp.enums;

import com.db.common.enums.NotificationOperator;

/**
 * Created by DB on 15-05-2017.
 */
public enum EntityType {
    ENTITY_TYPE_PERSON("PERSON"), ENTITY_TYPE_LOCATION("LOCATION"),ENTITY_TYPE_ORGANIZATION("ORGANIZATION"),ENTITY_TYPE_EVENT("EVENT");
    public String value;
    EntityType(String value) {
        this.value = value;
    }
    public static EntityType getEnumConstant(String value) {

        if (value != null) {
            for (EntityType enumConstant : EntityType.values()) {
                if (value.equalsIgnoreCase(enumConstant.value)) {
                    return enumConstant;
                }
            }
        }
        return null;
    }
    public String getValue() {
        return value;
    }
    public void setValue(String value) {
        this.value = value;
    }
}
