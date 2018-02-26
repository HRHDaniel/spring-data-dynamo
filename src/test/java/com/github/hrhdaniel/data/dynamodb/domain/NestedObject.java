package com.github.hrhdaniel.data.dynamodb.domain;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBDocument;

@DynamoDBDocument
public class NestedObject {
    
    @DynamoDBAttribute(attributeName="childName")
    private String subData;

    public void setSubData(String subData) {
        this.subData = subData;
    }

    public String getSubData() {
        return subData;
    }
}
