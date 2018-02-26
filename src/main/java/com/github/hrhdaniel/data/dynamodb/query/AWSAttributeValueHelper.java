package com.github.hrhdaniel.data.dynamodb.query;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Converts base java types into {@link com.amazonaws.services.dynamodbv2.model.AttributeValue} 
 * 
 * @author dharp
 */
public class AWSAttributeValueHelper {
    
    private AWSAttributeValueHelper() {
    }

    public static AttributeValue toAttributeValue(Object o) {
        if (o instanceof String) {
            return new AttributeValue().withS((String) o);
        } else if (o instanceof Boolean) {
            return new AttributeValue().withBOOL((Boolean) o);
        } else if (o instanceof ByteBuffer) {
            return new AttributeValue().withB((ByteBuffer) o);
        } else if (o instanceof Date) {
            LocalDateTime instant = LocalDateTime.ofInstant(((Date) o).toInstant(), ZoneId.of("UTC"));
            String dateString = DateTimeFormatter.ISO_LOCAL_DATE.format(instant);
            return new AttributeValue().withS(dateString);
        } else if (o instanceof Calendar) {
            LocalDateTime instant = LocalDateTime.ofInstant(((Calendar) o).toInstant(), ZoneId.of("UTC"));
            String dateString = DateTimeFormatter.ISO_LOCAL_DATE.format(instant);
            return new AttributeValue().withS(dateString);
        } else if (o instanceof Long) {
            return new AttributeValue().withN(o.toString());
        } else if (o instanceof Integer) {
            return new AttributeValue().withN(o.toString());
        } else if (o instanceof Double) {
            return new AttributeValue().withN(o.toString());
        } else if (o instanceof Float) {
            return new AttributeValue().withN(o.toString());
        } else if (o instanceof BigDecimal) {
            return new AttributeValue().withN(o.toString());
        } else if (o instanceof BigInteger) {
            return new AttributeValue().withN(o.toString());
        } else {
            throw new UnsupportedOperationException("Unable to create AttributeValue from type " + o.getClass().getName());
        }
    }
}
