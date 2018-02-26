package com.github.hrhdaniel.data.dynamodb.config;

import java.io.Serializable;

import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;

public class DynamoRepositoryFactoryBean<T extends Repository<S, I>, S, I extends Serializable>
        extends RepositoryFactoryBeanSupport<T, S, I> {
    
    private DynamoDBMapper mapper;

    protected DynamoRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
        super(repositoryInterface);
    }
    
    @Override
    protected RepositoryFactorySupport createRepositoryFactory() {
        return new DynamoRepositoryFactory(mapper);
    }

    public void setDynamoDBMapper(DynamoDBMapper mapper) {
        this.mapper = mapper;
    }
}
