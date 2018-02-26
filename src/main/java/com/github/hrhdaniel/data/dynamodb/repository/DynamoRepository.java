package com.github.hrhdaniel.data.dynamodb.repository;

import java.io.Serializable;

import org.springframework.data.repository.CrudRepository;

/**
 * 
 * @author dharp
 *
 * @param <T>
 *            Base type stored in repository
 * @param <I>
 *            ID of object. If Hash and Range keys are used, this should be
 *            {@link com.github.hrhdaniel.data.dynamodb.repository.DynamoCompositeKey}
 */
public interface DynamoRepository<T, I extends Serializable> extends CrudRepository<T, I> {

}
