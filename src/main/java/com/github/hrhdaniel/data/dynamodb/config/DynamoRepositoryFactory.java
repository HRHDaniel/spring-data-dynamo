package com.github.hrhdaniel.data.dynamodb.config;

import java.io.Serializable;

import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.ReflectionEntityInformation;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.github.hrhdaniel.data.dynamodb.query.DynamoQueryLookupStrategy;
import com.github.hrhdaniel.data.dynamodb.repository.DynamoCrudRepository;
import com.github.hrhdaniel.data.dynamodb.repository.DynamoRepository;

public class DynamoRepositoryFactory extends RepositoryFactorySupport {
    
    private DynamoDBMapper mapper;

    public DynamoRepositoryFactory(DynamoDBMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public <T, I extends Serializable> EntityInformation<T, I> getEntityInformation(Class<T> domainClass) {
        return new ReflectionEntityInformation<>(domainClass);
    }

    @Override
    protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
        return DynamoRepository.class;
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected Object getTargetRepository(RepositoryInformation metadata) {
        return new DynamoCrudRepository(metadata, mapper);
    }
    
    @Override
    protected QueryLookupStrategy getQueryLookupStrategy(Key key,
            EvaluationContextProvider evaluationContextProvider) {

        return new DynamoQueryLookupStrategy(mapper);
        
    }

}
