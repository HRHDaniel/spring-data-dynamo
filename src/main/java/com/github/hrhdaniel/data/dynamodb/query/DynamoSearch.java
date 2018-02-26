package com.github.hrhdaniel.data.dynamodb.query;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;

/**
 * A dynamo search operation.  Either a query or scan.
 * 
 * @author dharp
 */
public interface DynamoSearch {

    public <T> Object execute(DynamoDBMapper mapper, Class<T> clazz);

}
