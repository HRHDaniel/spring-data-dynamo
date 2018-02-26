package com.github.hrhdaniel.data.dynamodb.query.criteria;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.github.hrhdaniel.data.dynamodb.query.DynamoSearch;

/**
 * Executes a DynamoDB Query
 * 
 * @see {@link com.github.hrhdaniel.data.dynamodb.query.criteria.Scan}
 * @author dharp
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class Query implements DynamoSearch {
    
    private DynamoDBQueryExpression expression;

    public Query(Criteria keys, Criteria criteria, String indexName) {
        expression = new DynamoDBQueryExpression();
        expression.setKeyConditionExpression(keys.getFilterExpression(false));
        expression.setFilterExpression(criteria.getFilterExpression(true));

        Map<String, AttributeValue> attributeValues = new HashMap<>();
        attributeValues.putAll(keys.getAttributeValues(false));
        attributeValues.putAll(criteria.getAttributeValues(true));
        expression.setExpressionAttributeValues(attributeValues);
        
        if ( StringUtils.isNotEmpty(indexName) ) {
            expression.setIndexName(indexName);
            expression.setConsistentRead(false);
        }
    }

    @Override
    public <T> Object execute(DynamoDBMapper mapper, Class<T> clazz) {
        PaginatedQueryList results = mapper.query(clazz, expression);

        return results;
    }

}
