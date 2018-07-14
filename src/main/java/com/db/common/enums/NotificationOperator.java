package com.db.common.enums;

/**
 * Created by DB on 20-04-2017.
 */
public enum NotificationOperator {
    NOTIFICATION_OPERATOR_OR("OR"), NOTIFICATION_OPERATOR_AND("AND");
    public String value;
     NotificationOperator(String value) {
        this.value = value;
    }
    public static NotificationOperator getEnumConstant(String value) {

        if (value != null) {
            for (NotificationOperator enumConstant : NotificationOperator.values()) {
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



