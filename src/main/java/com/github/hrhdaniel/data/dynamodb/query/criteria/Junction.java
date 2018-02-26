package com.github.hrhdaniel.data.dynamodb.query.criteria;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Combines criteria, such as an AND or OR
 * 
 * @author dharp
 */
public abstract class Junction extends Criteria {

    private Criteria left;
    private Criteria right;

    public Junction(Criteria left, Criteria right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public Map<String, AttributeValue> getAttributeValues(boolean ignoreKeys) {
        if (ignoreKeys) {
            // Return nothing if we're both keys, otherwise if one of our sides is a
            // key, just return the other one
            if (left.isKey() && right.isKey()) {
                return new HashMap<>();
            } else if (left.isKey()) {
                return right.getAttributeValues(ignoreKeys);
            } else if (right.isKey()) {
                return left.getAttributeValues(ignoreKeys);
            }
        }
        Map<String, AttributeValue> attributeValues = new HashMap<>();
        attributeValues.putAll(left.getAttributeValues(ignoreKeys));
        attributeValues.putAll(right.getAttributeValues(ignoreKeys));
        return attributeValues;
    }

    @Override
    public String getFilterExpression(boolean ignoreKeys) {
        if (ignoreKeys) {
            // Return nothing if we're both keys, otherwise if one of our "AND" sides is a
            // key, just return the other one
            if (left.isKey() && right.isKey()) {
                return null;
            } else if (left.isKey()) {
                return right.getFilterExpression(ignoreKeys);
            } else if (right.isKey()) {
                return left.getFilterExpression(ignoreKeys);
            }
        }
        
        StringBuilder filter = new StringBuilder(" ( ");
        filter.append(left.getFilterExpression(ignoreKeys));
        filter.append(getJunctionStr());
        filter.append(right.getFilterExpression(ignoreKeys));
        filter.append(" ) ");
        return filter.toString();
    }

    @Override
    public boolean isKey() {
        return left.isKey() && right.isKey();
    }

    protected abstract String getJunctionStr();
}
