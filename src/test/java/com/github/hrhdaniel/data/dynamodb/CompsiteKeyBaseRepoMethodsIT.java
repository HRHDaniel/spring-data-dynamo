package com.github.hrhdaniel.data.dynamodb;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.assertj.core.util.Lists;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsNull;
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
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.github.hrhdaniel.data.dynamodb.CompsiteKeyBaseRepoMethodsIT.TestConfig;
import com.github.hrhdaniel.data.dynamodb.config.EnableDynamoRepositories;
import com.github.hrhdaniel.data.dynamodb.domain.CompositeKeyObject;
import com.github.hrhdaniel.data.dynamodb.domain.NestedObject;
import com.github.hrhdaniel.data.dynamodb.repositories.CompositeKeyRepository;
import com.github.hrhdaniel.data.dynamodb.repository.DynamoCompositeKey;

import testutils.SpringInitializationListener;
import testutils.SpringWrappedTestRunner;

@RunWith(SpringWrappedTestRunner.class)
@ContextConfiguration(classes = { TestConfig.class })
@SpringBootTest
public class CompsiteKeyBaseRepoMethodsIT implements SpringInitializationListener {

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

        CreateTableRequest createTableRequest = mapper.generateCreateTableRequest(CompositeKeyObject.class);
        createTableRequest.setProvisionedThroughput(
                new ProvisionedThroughput().withReadCapacityUnits(5L).withWriteCapacityUnits(5L));
        Table table = dynamoDB.createTable(createTableRequest);

        try {
            table.waitForActive();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private CompositeKeyObject createOne(String name, int size, String detail) {
        CompositeKeyObject entity = new CompositeKeyObject();
        entity.setObjectName(name);
        entity.setObjectSize(size);
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
            createOne("Kit Cloudkicker", 1, "Air surfing"),
            createOne("Baloo", 2, "Pilot"),
            createOne("Molly Cunningham", 1, "Button Nose"));

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
    public void testCount() {
        assertThat(repository.count(), is((long) loadedTestData.size()));
    }

    @Test
    public void testDeleteByID() {
        // Given
        DynamoCompositeKey<String, Integer> id = new DynamoCompositeKey<>();
        id.setHashKey("Baloo");
        id.setRangeKey(2);

        // When
        repository.delete(id);

        // Then
        assertThat(repository.findOne(id), IsNull.nullValue());
    }

    @Test
    public void testDeleteByInstance() {
        // Given
        CompositeKeyObject instance = new CompositeKeyObject();
        instance.setObjectName("Baloo");
        instance.setObjectSize(2);

        // When
        repository.delete(instance);

        // Then
        assertThat(repository.findOne(instance.getId()), IsNull.nullValue());
    }

    @Test
    public void testDeleteItems() {
        repository.delete(loadedTestData);

        loadedTestData.forEach(e -> {
            assertThat(repository.findOne(e.getId()), IsNull.nullValue());
        });
    }

    @Test
    public void testDeleteAll() {
        repository.deleteAll();

        loadedTestData.forEach(e -> {
            assertThat(repository.findOne(e.getId()), IsNull.nullValue());
        });
    }

    @Test
    public void testExists() {
        boolean exists = repository.exists(loadedTestData.get(0).getId());

        assertThat(exists, is(true));
    }

    @Test
    public void testFindAll() {
        ArrayList<CompositeKeyObject> found = Lists.newArrayList(repository.findAll());

        assertThat(found, containsInAnyOrder(loadedTestData.toArray()));
    }

    @Test
    public void testFindAllInIDList() {
        List<CompositeKeyObject> lookForItems = loadedTestData.stream().limit(2).collect(Collectors.toList());
        List<DynamoCompositeKey<String, Integer>> searchIDs = lookForItems.stream().map(e -> e.getId())
                .collect(Collectors.toList());

        ArrayList<CompositeKeyObject> found = Lists.newArrayList(repository.findAll(searchIDs));

        assertThat(found, containsInAnyOrder(lookForItems.toArray()));
    }

    @Test
    public void testFindOne() {
        CompositeKeyObject expected = loadedTestData.get(0);

        CompositeKeyObject found = repository.findOne(expected.getId());

        assertThat(found, is(expected));
    }

    @Test
    public void testSaveItem() {
        CompositeKeyObject created = createOne("Don Karnage", 4, "Pirate");

        repository.save(created);

        assertThat(repository.findOne(created.getId()), is(created));
    }

    @Test
    public void testSaveItems() {
        List<CompositeKeyObject> newItems = Arrays.asList(
                createOne("Don Karnage", 4, "Pirate"),
                createOne("Mad Dog", 4, "Pirate"));

        repository.save(newItems);

        Iterable<CompositeKeyObject> all = repository.findAll();

        List<CompositeKeyObject> expected = new ArrayList<>(loadedTestData);
        expected.addAll(newItems);

        assertThat(all, containsInAnyOrder(expected.toArray()));
    }
}
