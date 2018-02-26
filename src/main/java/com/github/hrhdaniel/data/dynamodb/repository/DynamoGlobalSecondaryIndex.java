package com.github.hrhdaniel.data.dynamodb.repository;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Sets a method on a repository to use a GlobalSecondaryIndex
 *  
 * @author dharp
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DynamoGlobalSecondaryIndex {
    
    String indexName();
    
    String hashKeyName();
    
    String rangeKeyName() default "";

}
