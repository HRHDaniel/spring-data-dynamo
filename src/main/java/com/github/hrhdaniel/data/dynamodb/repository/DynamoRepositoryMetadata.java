package com.github.hrhdaniel.data.dynamodb.repository;

import java.lang.reflect.Method;

import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.util.ReflectionUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;

public class DynamoRepositoryMetadata {

    private boolean isCompositeKey;
    private String hashKeyPropertyName;
    private String rangeKeyPropertyName;
    private RepositoryMetadata springMetadata;

    public DynamoRepositoryMetadata(final RepositoryMetadata metadata) {
        this.springMetadata = metadata;
        isCompositeKey = false;

        ReflectionUtils.doWithFields(metadata.getDomainType(), field -> {
            if ( field.isAnnotationPresent(DynamoDBHashKey.class) ) {
                hashKeyPropertyName = field.getName();
            }
            if ( field.isAnnotationPresent(DynamoDBRangeKey.class) ) {
                rangeKeyPropertyName = field.getName();
            }
        });

        ReflectionUtils.doWithMethods(metadata.getDomainType(), method -> {
            if (method.getAnnotation(DynamoDBHashKey.class) != null) {
                hashKeyPropertyName = getPropertyNameForAccessorMethod(method);
            }
            if (method.getAnnotation(DynamoDBRangeKey.class) != null) {
                rangeKeyPropertyName = getPropertyNameForAccessorMethod(method);
                isCompositeKey = true;
            }
        });
    }

    protected String getPropertyNameForAccessorMethod(Method method) {
        String methodName = method.getName();

        // Strip off bean prefix (get, is, has...) and lowercase first letter
        char[] propertyName = methodName.replaceAll("^[a-z]+", "").toCharArray();
        propertyName[0] = Character.toLowerCase(propertyName[0]);

        return new String(propertyName);
    }

    public boolean isCompositeKey() {
        return isCompositeKey;
    }

    public String getHashKeyPropertyName() {
        return hashKeyPropertyName;
    }

    public String getRangeKeyPropertyName() {
        return rangeKeyPropertyName;
    }

    public RepositoryMetadata getSpringMetadata() {
        return springMetadata;
    }

}
