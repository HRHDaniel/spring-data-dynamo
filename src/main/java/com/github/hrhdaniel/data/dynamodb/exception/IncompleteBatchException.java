package com.github.hrhdaniel.data.dynamodb.exception;

import java.util.List;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper.FailedBatch;

public class IncompleteBatchException extends RuntimeException {

    private static final long serialVersionUID = 3848061600406382447L;
    
    private final transient List<FailedBatch> failedItems;

    public IncompleteBatchException(List<FailedBatch> failedItems) {
        super();
        this.failedItems = failedItems;
    }
    
    public List<FailedBatch> getFailedItems() {
        return failedItems;
    }
    
}
