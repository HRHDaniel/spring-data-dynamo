package com.github.hrhdaniel.data.dynamodb.query;

import java.lang.reflect.Method;

import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;

public class DynamoQueryLookupStrategy implements QueryLookupStrategy {

    private DynamoDBMapper mapper;
    
    public DynamoQueryLookupStrategy(DynamoDBMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
            NamedQueries namedQueries) {
        // TODO Named Queries?
        
        return createDynamoDBQuery(method, metadata, factory, metadata.getDomainType(), metadata.getIdType());
    }
    
    protected <T, I> RepositoryQuery createDynamoDBQuery(Method method,
            RepositoryMetadata metadata, ProjectionFactory factory, Class<T> entityClass, Class<I> idClass) {
        return new DynamoRepositoryQuery<>(method, metadata, factory, mapper, metadata.getDomainType(), metadata.getIdType());
    }

}
