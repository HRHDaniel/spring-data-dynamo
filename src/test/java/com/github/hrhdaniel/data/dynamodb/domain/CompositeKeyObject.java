package com.github.hrhdaniel.data.dynamodb.domain;

import java.util.List;

import org.springframework.data.annotation.Id;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel.DynamoDBAttributeType;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTyped;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.hrhdaniel.data.dynamodb.repository.DynamoCompositeKey;

@DynamoDBTable(tableName = "testtable")
public class CompositeKeyObject {

    @Id
    @JsonIgnore
    @DynamoDBIgnore
    private final DynamoCompositeKey<String, Integer> id;

    private NestedObject nested;

    private String gender;
    
    private List<String> extraStrings;
    
    @DynamoDBTyped(DynamoDBAttributeType.BOOL)
    private Boolean bear;

    public CompositeKeyObject() {
        id = new DynamoCompositeKey<>();
    }

    @DynamoDBHashKey
    public String getObjectName() {
        return id.getHashKey();
    }

    public void setObjectName(String objectName) {
        id.setHashKey(objectName);
    }

    @DynamoDBRangeKey
    public int getObjectSize() {
        return id.getRangeKey();
    }

    public void setObjectSize(int size) {
        id.setRangeKey(size);
    }

    public NestedObject getNested() {
        return nested;
    }

    public void setNested(NestedObject nested) {
        this.nested = nested;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }
    
    public Boolean isBear() {
        return bear;
    }

    public void setBear(Boolean bear) {
        this.bear = bear;
    }
    
    public List<String> getExtraStrings() {
        return extraStrings;
    }

    public void setExtraStrings(List<String> extraStrings) {
        this.extraStrings = extraStrings;
    }
    
    public DynamoCompositeKey<String, Integer> getId() {
        // Don't really need this on our objects, but I am using this to make tests
        // easier
        return id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id.getHashKey() == null) ? 0 : id.getHashKey().hashCode());
        result = prime * result + ((id.getRangeKey() == null) ? 0 : id.getRangeKey().hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CompositeKeyObject other = (CompositeKeyObject) obj;
        if (id.getHashKey() == null) {
            if (other.id.getHashKey() != null)
                return false;
        } else if (!id.getHashKey().equals(other.id.getHashKey()))
            return false;
        if (id.getRangeKey() == null) {
            if (other.id.getRangeKey() != null)
                return false;
        } else if (!id.getRangeKey().equals(other.id.getRangeKey()))
            return false;
        return true;
    }


}
