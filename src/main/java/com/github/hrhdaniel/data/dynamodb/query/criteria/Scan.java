package com.github.hrhdaniel.data.dynamodb.query.criteria;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedList;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.github.hrhdaniel.data.dynamodb.query.DynamoSearch;

/**
 * Executes a DynamoDB Scan
 * 
 * @see {@link com.github.hrhdaniel.data.dynamodb.query.criteria.Query}
 * @author dharp
 */
@SuppressWarnings("rawtypes")
public class Scan implements DynamoSearch {

    private DynamoDBScanExpression expression;

    public Scan(Criteria criteria, String indexName) {
        expression = new DynamoDBScanExpression();
        expression.setFilterExpression(criteria.getFilterExpression(false));

        Map<String, AttributeValue> attributeValues = new HashMap<>();
        attributeValues.putAll(criteria.getAttributeValues(false));
        expression.setExpressionAttributeValues(attributeValues);
        
        if ( StringUtils.isNotEmpty(indexName) ) {
            expression.setIndexName(indexName);
            expression.setConsistentRead(false);
        }
    }

    @Override
    public <T> Object execute(DynamoDBMapper mapper, Class<T> clazz) {
        
        PaginatedList results = mapper.scan(clazz, expression);
        
        return results;
    }

}
