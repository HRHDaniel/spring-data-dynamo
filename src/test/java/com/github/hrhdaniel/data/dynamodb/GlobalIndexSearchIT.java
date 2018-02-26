package com.github.hrhdaniel.data.dynamodb;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.github.hrhdaniel.data.dynamodb.GlobalIndexSearchIT.TestConfig;
import com.github.hrhdaniel.data.dynamodb.config.EnableDynamoRepositories;
import com.github.hrhdaniel.data.dynamodb.domain.CompositeKeyObject;
import com.github.hrhdaniel.data.dynamodb.domain.NestedObject;
import com.github.hrhdaniel.data.dynamodb.repositories.CompositeKeyRepository;

import testutils.SpringInitializationListener;
import testutils.SpringWrappedTestRunner;

@RunWith(SpringWrappedTestRunner.class)
@ContextConfiguration(classes = { TestConfig.class })
@SpringBootTest
public class GlobalIndexSearchIT implements SpringInitializationListener {

    @SpringBootConfiguration
    @EnableDynamoRepositories
    public static class TestConfig {
        @Bean
        public AmazonDynamoDB amazonDynamoDB() {
            return AmazonDynamoDBClientBuilder.standard().withCredentials(awsCredentailsProvider())
                    .withRegion(Regions.US_WEST_2)
                    .build();
        }

        @Bean
        public AWSCredentialsProvider awsCredentailsProvider() {
            return new DefaultAWSCredentialsProviderChain();
        }

        @Bean
        public DynamoDBMapperConfig dynamoDbMapperConfig() {
            return new DynamoDBMapperConfig.Builder().build();
        }

        @Bean
        public DynamoDBMapper dynamoDBMapper() {
            return new DynamoDBMapper(amazonDynamoDB());
        }
    }

    @Override
    public void afterClass() {
        DynamoDB dynamoDB = new DynamoDB(client);
        DeleteTableRequest deleteTableRequest = mapper.generateDeleteTableRequest(CompositeKeyObject.class);
        client.deleteTable(deleteTableRequest);
        Table table = dynamoDB.getTable(deleteTableRequest.getTableName());

        try {
            table.waitForDelete();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void beforeClass() {
        DynamoDB dynamoDB = new DynamoDB(client);
        assertThat(client, CoreMatchers.notNullValue());

        ProvisionedThroughput throughput = new ProvisionedThroughput().withReadCapacityUnits(5L)
                .withWriteCapacityUnits(5L);

        GlobalSecondaryIndex globalSecondaryIndex = new GlobalSecondaryIndex()
                .withIndexName("testgsi")
                .withKeySchema(new KeySchemaElement().withAttributeName("gender").withKeyType(KeyType.HASH))
                .withProvisionedThroughput(throughput)
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL));

        CreateTableRequest createTableRequest = mapper.generateCreateTableRequest(CompositeKeyObject.class)
                .withProvisionedThroughput(throughput)
                .withGlobalSecondaryIndexes(globalSecondaryIndex)
                .withAttributeDefinitions(new AttributeDefinition().withAttributeName("gender").withAttributeType("S"));

        Table table = dynamoDB.createTable(createTableRequest);

        try {
            table.waitForActive();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private CompositeKeyObject createOne(String name, int size, String gender, String detail) {
        CompositeKeyObject entity = new CompositeKeyObject();
        entity.setObjectName(name);
        entity.setObjectSize(size);
        entity.setGender(gender);
        NestedObject nested = new NestedObject();
        nested.setSubData(detail);
        entity.setNested(nested);
        return entity;
    }

    @Autowired
    private AmazonDynamoDB client;

    @Autowired
    private DynamoDBMapper mapper;

    @Autowired
    private CompositeKeyRepository repository;

    private List<CompositeKeyObject> loadedTestData = Arrays.asList(
            createOne("Kit Cloudkicker", 1, "M", "Air surfing"),
            createOne("Baloo", 2, "M", "Pilot"),
            createOne("Molly Cunningham", 1, "F", "Button Nose"));

    @Before
    public void beforeTest() {
        // Yeah, using the thing that we're trying to test, but yeah, I'm just going to
        // do that throughout this class because I'm lazy today
        loadedTestData.forEach(i -> repository.save(i));
    }

    @After
    public void afterTest() {
        repository.deleteAll();
    }

    @Test
    public void testIndexQuery() {
        // So, this is annotated with the Index... I saw that it did the right thing by
        // putting in some debug points, but how would we programmatically test that
        // it's using a query on the index instead of a scan on the table?
        List<CompositeKeyObject> items = repository.findByGender("F");

        assertThat(items.size(), is(1));
        assertThat(items.get(0).getObjectName(), is("Molly Cunningham"));

    }
}
