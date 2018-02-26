package com.github.hrhdaniel.data.dynamodb.query.criteria;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Base class that represents criteria to apply in a filter or key expression
 * 
 * @author dharp
 */
public abstract class Criteria {

    /**
     * The filter expression string
     * 
     * @param ignoreKeys
     *            whether or not to include criteria on the keys (hash or
     *            sort/range) in this filter
     * @return
     */
    public abstract String getFilterExpression(boolean ignoreKeys);

    /**
     * Map of the an attribute names in the filter expressions, such as ":v1", to
     * the AttributeValues
     * 
     * @param ignoreKeys
     *            whether or not to include criteria on the keys (hash or
     *            sort/range) in this filter
     * @return
     */
    public abstract Map<String, AttributeValue> getAttributeValues(boolean ignoreKeys);

    /**
     * Is this criteria applied to a key (hash or sort/range) for the query/scan
     * 
     * @return
     */
    public boolean isKey() {
        return false;
    }

}
