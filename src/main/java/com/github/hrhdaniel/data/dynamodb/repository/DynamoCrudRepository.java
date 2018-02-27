package com.github.hrhdaniel.data.dynamodb.repository;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.springframework.data.annotation.Id;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.util.Assert;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper.FailedBatch;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.github.hrhdaniel.data.dynamodb.exception.IncompleteBatchException;

/**
 * DynamoDB Repository Implementation
 * 
 * @author dharp
 *
 * @param <T>
 *            Base table item type
 * @param <I>
 *            Key type - should be a DynamoCompositeKey if both a HashKey and
 *            RangeKey are used
 */
public class DynamoCrudRepository<T, I extends Serializable> implements DynamoRepository<T, I> {

    private DynamoDBMapper mapper;
    private Class<T> domainType;

    @SuppressWarnings("unchecked")
    public DynamoCrudRepository(RepositoryInformation metadata, DynamoDBMapper mapper) {
        this.mapper = mapper;

        domainType = (Class<T>) metadata.getDomainType();
    }

    @Override
    public long count() {
        return mapper.count(domainType, new DynamoDBScanExpression());
    }

    @Override
    public void delete(I id) {
        T idObject = createIDInstance(id);
        delete(idObject);
    }

    @Override
    public void delete(T item) {
        mapper.delete(item);
    }

    @Override
    public void delete(Iterable<? extends T> items) {
        List<FailedBatch> failedBatchList = mapper.batchDelete(items);
        if (!failedBatchList.isEmpty()) {
            throw new IncompleteBatchException(failedBatchList);
        }
    }

    @Override
    public void deleteAll() {
        delete(findAll());
    }

    @Override
    public boolean exists(I id) {
        return null != findOne(id);
    }

    @Override
    public Iterable<T> findAll() {
        PaginatedScanList<T> scan = mapper.scan(domainType, new DynamoDBScanExpression());
        return scan;
    }

    @Override
    public Iterable<T> findAll(Iterable<I> ids) {
        List<T> idList = StreamSupport.stream(ids.spliterator(), false)
                .map(this::createIDInstance)
                .collect(Collectors.toList());
        return findAllByItems(idList);
    }

    @Override
    public T findOne(I id) {
        if (id instanceof DynamoCompositeKey) {
            DynamoCompositeKey<?, ?> key = (DynamoCompositeKey<?, ?>) id;
            return mapper.load(domainType, key.getHashKey(), key.getRangeKey());
        } else {
            return mapper.load(domainType, id);
        }
    }

    @Override
    public <S extends T> S save(S item) {
        mapper.save(item);
        return mapper.load(item);
    }

    @Override
    public <S extends T> Iterable<S> save(Iterable<S> items) {
        List<FailedBatch> failedBatchList = mapper.batchSave(items);
        if (!failedBatchList.isEmpty()) {
            throw new IncompleteBatchException(failedBatchList);
        }
        return findAllByItems(items);
    }

    private T createIDInstance(I id) {
        try {
            T newInstance = domainType.newInstance();
            Field[] fieldsWithAnnotation = FieldUtils.getFieldsWithAnnotation(domainType, Id.class);
            Assert.isTrue(fieldsWithAnnotation.length < 2,
                    "More than 1 fields annotated as @Id for class " + domainType.getName());
            if (fieldsWithAnnotation.length == 1) {
                FieldUtils.writeField(fieldsWithAnnotation[0], newInstance, id, true);
                return newInstance;
            }

            Method[] methodsWithAnnotation = MethodUtils.getMethodsWithAnnotation(domainType, Id.class);
            Assert.isTrue(methodsWithAnnotation.length < 2,
                    "More than 1 fields annotated as @Id for class " + domainType.getName());
            if (methodsWithAnnotation.length != 1) {
                throw new IllegalStateException("Could not find @Id field/method for " + domainType.getName());
            }

            methodsWithAnnotation[0].invoke(newInstance, id);
            return newInstance;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "ReflectiveOperationException trying to create instance, find @Id, or set @Id on type "
                            + domainType.getName(),
                    e);
        }
    }

    @SuppressWarnings("unchecked")
    private <S extends T> Iterable<S> findAllByItems(Iterable<S> items) {
        Map<String, List<Object>> batch = mapper.batchLoad(items);

        Assert.isTrue(batch.size() < 2, "Find all returned results from multiple tables???");
        if (batch.size() == 0) {
            return Collections.emptyList();
        }

        return (Iterable<S>) batch.values().iterator().next();
    }

}
