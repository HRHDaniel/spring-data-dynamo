package com.github.hrhdaniel.data.dynamodb.repository;

import java.io.Serializable;

/**
 * Represents a composite key in DynamoDB
 * 
 * @author dharp
 *
 * @param <H>
 *            The HashKey Type
 * @param <R>
 *            The RangeKey Type
 */
public class DynamoCompositeKey<H extends Serializable, R extends Serializable> implements Serializable {

    private static final long serialVersionUID = -4075725327295377200L;

    private H hashKey;
    private R rangeKey;

    public H getHashKey() {
        return hashKey;
    }

    public void setHashKey(H hashKey) {
        this.hashKey = hashKey;
    }

    public R getRangeKey() {
        return rangeKey;
    }

    public void setRangeKey(R rangeKey) {
        this.rangeKey = rangeKey;
    }

}
