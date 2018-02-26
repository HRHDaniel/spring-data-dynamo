package com.github.hrhdaniel.data.dynamodb.repositories;

import com.github.hrhdaniel.data.dynamodb.domain.HashKeyOnlyTestObject;
import com.github.hrhdaniel.data.dynamodb.repository.DynamoRepository;

public interface HashKeyOnlyRepository extends DynamoRepository<HashKeyOnlyTestObject, String> {

}
