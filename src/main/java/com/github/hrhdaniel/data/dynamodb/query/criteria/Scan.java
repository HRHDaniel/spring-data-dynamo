package com.github.hrhdaniel.data.dynamodb.query.criteria;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    
    private static final Logger LOG = LoggerFactory.getLogger(Scan.class);

    private DynamoDBScanExpression expression;

    private Set<String> scanReasons;

    public Scan(Criteria criteria, String indexName, Set<String> scanReasons) {
        this.scanReasons = scanReasons;
        expression = new DynamoDBScanExpression();
        expression.setFilterExpression(criteria.getFilterExpression(false));

        Map<String, AttributeValue> attributeValues = new HashMap<>();
        attributeValues.putAll(criteria.getAttributeValues(false));
        
        if ( !attributeValues.isEmpty() ) {
            expression.setExpressionAttributeValues(attributeValues);
        }
        
        if ( StringUtils.isNotEmpty(indexName) ) {
            expression.setIndexName(indexName);
            expression.setConsistentRead(false);
        }
    }

    @Override
    public <T> Object execute(DynamoDBMapper mapper, Class<T> clazz) {
        LOG.debug("Executing Dynamo Scan with Filter [{}] Index [{}].  Scanning due to {}",
                expression.getFilterExpression(), expression.getIndexName(), scanReasons);

        PaginatedList results = mapper.scan(clazz, expression);
        
        return results;
    }

}
