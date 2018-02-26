package com.github.hrhdaniel.data.dynamodb.config;

import java.lang.annotation.Annotation;

import org.springframework.data.repository.config.RepositoryBeanDefinitionRegistrarSupport;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;

public class DynamoRepositoriesRegistrar extends RepositoryBeanDefinitionRegistrarSupport {

    @Override
    protected Class<? extends Annotation> getAnnotation() {
        return EnableDynamoRepositories.class;
    }

    @Override
    protected RepositoryConfigurationExtension getExtension() {
        return new DynamoRepositoryConfigurationExtensionSupport();
    }
}
