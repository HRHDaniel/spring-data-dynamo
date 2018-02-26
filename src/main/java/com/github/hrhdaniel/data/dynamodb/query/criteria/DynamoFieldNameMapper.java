package com.github.hrhdaniel.data.dynamodb.query.criteria;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.MethodDescriptor;
import java.lang.reflect.Field;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.util.ReflectionUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;

/**
 * Helps scan for the dynamo mapping annotations that specify what the name is
 * in the database if different than the java field
 * 
 * @author dharp
 */
public class DynamoFieldNameMapper {
    
    private DynamoFieldNameMapper() {
    }

    public static String mapToDynamoName(PropertyPath propertyPath) {

        // Each segment represents a field in a class, which means we need to find the
        // name for each one, then combine them by dots
        return StreamSupport.stream(propertyPath.spliterator(), false)
                .map(p -> mapToDynamoName(p.getSegment(), p.getOwningType().getType()))
                .collect(Collectors.joining("."));
    }

    private static String mapToDynamoName(String propertyName, Class<?> type) {
        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(type);
            MethodDescriptor[] methodDescriptors = beanInfo.getMethodDescriptors();
            for (MethodDescriptor m : methodDescriptors) {
                if (propertyName.equals(stripBeanMethodPrefix(m.getName()))) {
                    if (m.getMethod().getAnnotation(DynamoDBAttribute.class) != null
                            && !StringUtils
                                    .isEmpty(m.getMethod().getAnnotation(DynamoDBAttribute.class).attributeName())) {
                        return m.getMethod().getAnnotation(DynamoDBAttribute.class).attributeName();
                    } else if (m.getMethod().getAnnotation(DynamoDBHashKey.class) != null
                            && !StringUtils
                                    .isEmpty(m.getMethod().getAnnotation(DynamoDBHashKey.class).attributeName())) {
                        return m.getMethod().getAnnotation(DynamoDBHashKey.class).attributeName();
                    } else if (m.getMethod().getAnnotation(DynamoDBRangeKey.class) != null
                            && !StringUtils
                                    .isEmpty(m.getMethod().getAnnotation(DynamoDBRangeKey.class).attributeName())) {
                        return m.getMethod().getAnnotation(DynamoDBRangeKey.class).attributeName();
                    }
                }
            }

            Field field = ReflectionUtils.findField(type, propertyName);
            if (field != null) {
                if (field.getAnnotation(DynamoDBAttribute.class) != null
                        && !StringUtils.isEmpty(field.getAnnotation(DynamoDBAttribute.class).attributeName())) {
                    return field.getAnnotation(DynamoDBAttribute.class).attributeName();
                } else if (field.getAnnotation(DynamoDBHashKey.class) != null
                        && !StringUtils.isEmpty(field.getAnnotation(DynamoDBHashKey.class).attributeName())) {
                    return field.getAnnotation(DynamoDBHashKey.class).attributeName();
                } else if (field.getAnnotation(DynamoDBRangeKey.class) != null
                        && !StringUtils.isEmpty(field.getAnnotation(DynamoDBRangeKey.class).attributeName())) {
                    return field.getAnnotation(DynamoDBRangeKey.class).attributeName();
                }
            }

        } catch (IntrospectionException e) {
            throw new IllegalStateException("Threw exception looking for dynamo annotations.  That is likely a legitimate bug and there's something I didn't account for that you need to fix.");
        }

        return propertyName;
    }

    /*
     * Basically drops the first lowcase part of a method name and lowercase the
     * first of the remainder:
     * getField -> field
     * getLongField -> longField
     * setField -> field
     * isSomething -> something
     */
    private static String stripBeanMethodPrefix(String methodName) {
        char[] c = methodName.replaceAll("^[a-z]+", "").toCharArray();
        if (c.length > 0) {
            c[0] = Character.toLowerCase(c[0]);
        }
        return new String(c);
    }

}
