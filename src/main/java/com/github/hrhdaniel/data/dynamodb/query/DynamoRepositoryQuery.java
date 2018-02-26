package com.github.hrhdaniel.data.dynamodb.query;

import java.lang.reflect.Method;

import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.github.hrhdaniel.data.dynamodb.repository.DynamoRepositoryMetadata;

public class DynamoRepositoryQuery<T, I> implements RepositoryQuery {

    private Method method;
    private RepositoryMetadata metadata;
    private DynamoRepositoryMetadata dynamoRepoMetadata;
    private DynamoDBMapper mapper;
    private PartTree tree;
    private QueryMethod queryMethod;

    public DynamoRepositoryQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
            DynamoDBMapper mapper, Class<T> entityClass, Class<I> idClass) {

        this.queryMethod = new QueryMethod(method, metadata, factory);

        this.metadata = metadata;
        this.mapper = mapper;
        this.method = method;

        this.tree = new PartTree(method.getName(), queryMethod.getEntityInformation().getJavaType());

        dynamoRepoMetadata = new DynamoRepositoryMetadata(metadata);
    }

    @Override
    public Object execute(Object[] parameters) {

        if (tree.isDistinct()) {
            throw new UnsupportedOperationException("Use of Distinct in dynamic queries is not supported.");
        }

        if (tree.isDelete()) {
            throw new UnsupportedOperationException("Use of Delete or Remove in dynamic queries is not supported");
        }

        if (tree.isCountProjection()) {
            throw new UnsupportedOperationException("Use of count in dynamic queries is not supported");
        }

        if (tree.isExistsProjection()) {
            throw new UnsupportedOperationException("Use of 'existing' in dynamic queries is not supported");
        }

        DynamoSearch search = new DynamoQueryCreator(method, dynamoRepoMetadata, tree, new ParametersParameterAccessor(
                queryMethod.getParameters(), parameters)).createQuery();

        return search.execute(mapper, metadata.getDomainType());
    }

    @Override
    public QueryMethod getQueryMethod() {
        return queryMethod;
    }

}
