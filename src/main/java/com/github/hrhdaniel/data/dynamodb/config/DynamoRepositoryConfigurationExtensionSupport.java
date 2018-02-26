package com.github.hrhdaniel.data.dynamodb.config;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;

public class DynamoRepositoryConfigurationExtensionSupport extends RepositoryConfigurationExtensionSupport {

    @Override
    public String getRepositoryFactoryClassName() {
        return DynamoRepositoryFactoryBean.class.getName();
    }

    @Override
    protected String getModulePrefix() {
        return "dynamo";
    }
    
    @Override
    public void postProcess(BeanDefinitionBuilder builder, AnnotationRepositoryConfigurationSource config) {
        builder.addPropertyReference("dynamoDBMapper", config.getAttribute("dynamoDBMapperRef"));
    }

}
