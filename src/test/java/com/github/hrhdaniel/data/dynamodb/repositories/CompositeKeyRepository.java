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
    
}
