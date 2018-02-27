package com.github.hrhdaniel.data.dynamodb.repositories;

import java.util.List;

import com.github.hrhdaniel.data.dynamodb.domain.CompositeKeyObject;
import com.github.hrhdaniel.data.dynamodb.repository.DynamoCompositeKey;
import com.github.hrhdaniel.data.dynamodb.repository.DynamoGlobalSecondaryIndex;
import com.github.hrhdaniel.data.dynamodb.repository.DynamoRepository;

public interface CompositeKeyRepository extends DynamoRepository<CompositeKeyObject, DynamoCompositeKey<String, Integer>> {

    @DynamoGlobalSecondaryIndex(hashKeyName="gender", indexName="testgsi")
    List<CompositeKeyObject> findByGender(String gender);
    
    List<CompositeKeyObject> findByNestedSubData(String subData);
    
    List<CompositeKeyObject> findByObjectNameIsNot(String name);
    
    List<CompositeKeyObject> findByGenderAndObjectSizeLessThan(String gender, int rangeKey);

    List<CompositeKeyObject> findByObjectNameStartsWith(String name);
    
    List<CompositeKeyObject> findByBearIsFalse();

    List<CompositeKeyObject> findByBearIsTrue();

    List<CompositeKeyObject> findByObjectNameIn(List<String> names);
    
    List<CompositeKeyObject> findByObjectNameNotIn(List<String> names);
    
    List<CompositeKeyObject> findByObjectNameIn(String [] names);
    
    List<CompositeKeyObject> findByObjectNameOrObjectName(String name1, String name2);
    
    List<CompositeKeyObject> findByObjectSizeBetween(int sizeLow, int sizeHigh);

    List<CompositeKeyObject> findByObjectSizeGreaterThanEqual(int size);

    List<CompositeKeyObject> findByObjectSizeGreaterThan(int size);
    
    List<CompositeKeyObject> findByObjectSizeLessThan(int size);
    
    List<CompositeKeyObject> findByObjectSizeLessThanEqual(int size);
    
    List<CompositeKeyObject> findByNestedSubDataExists();
    
    List<CompositeKeyObject> findByNestedSubDataIsNull();
    
    List<CompositeKeyObject> findByExtraStringsContains(String string);
    
    List<CompositeKeyObject> findByExtraStringsNotContains(String string);
    
}