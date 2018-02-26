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
import com.github.hrhdaniel.data.dynamodb.HashKeyOnlyBaseRepoMethodsIT.TestConfig;
import com.github.hrhdaniel.data.dynamodb.config.EnableDynamoRepositories;
import com.github.hrhdaniel.data.dynamodb.domain.HashKeyOnlyTestObject;
import com.github.hrhdaniel.data.dynamodb.domain.NestedObject;
import com.github.hrhdaniel.data.dynamodb.repositories.HashKeyOnlyRepository;

import testutils.SpringInitializationListener;
import testutils.SpringWrappedTestRunner;

@RunWith(SpringWrappedTestRunner.class)
@ContextConfiguration(classes = { TestConfig.class })
@SpringBootTest
public class HashKeyOnlyBaseRepoMethodsIT implements SpringInitializationListener {

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
        DeleteTableRequest deleteTableRequest = mapper.generateDeleteTableRequest(HashKeyOnlyTestObject.class);
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

        CreateTableRequest createTableRequest = mapper.generateCreateTableRequest(HashKeyOnlyTestObject.class);
        createTableRequest.setProvisionedThroughput(
                new ProvisionedThroughput().withReadCapacityUnits(5L).withWriteCapacityUnits(5L));
        Table table = dynamoDB.createTable(createTableRequest);

        try {
            table.waitForActive();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private HashKeyOnlyTestObject createOne(String name, String detail) {
        HashKeyOnlyTestObject entity = new HashKeyOnlyTestObject();
        entity.setHashKey(name);
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
    private HashKeyOnlyRepository repository;
    
    private List<HashKeyOnlyTestObject> loadedTestData = Arrays.asList(
            createOne("Kit Cloudkicker", "Air surfing"), 
            createOne("Baloo", "Pilot"),
            createOne("Molly Cunningham", "Button Nose")
            ); 

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
        assertThat(repository.count(), is((long)loadedTestData.size()));
    }

    @Test
    public void testDeleteByID() {
        repository.delete("Baloo");

        assertThat(repository.findOne("Baloo"), IsNull.nullValue());
    }

    @Test
    public void testDeleteByInstance() {
        HashKeyOnlyTestObject instance = createOne("Baloo", null);
        repository.delete(instance);

        assertThat(repository.findOne("Baloo"), IsNull.nullValue());
    }
    
    @Test
    public void testDeleteItems() {
        repository.delete(loadedTestData);
        
        loadedTestData.forEach(e -> {
            assertThat(repository.findOne(e.getHashKey()), IsNull.nullValue());
        });
    }
    
    @Test
    public void testDeleteAll() {
        repository.deleteAll();
        
        loadedTestData.forEach(e -> {
            assertThat(repository.findOne(e.getHashKey()), IsNull.nullValue());
        });
    }
    
    @Test
    public void testExists() {
        boolean exists = repository.exists(loadedTestData.get(0).getHashKey());
        
        assertThat(exists, is(true));
    }
    
    @Test
    public void testFindAll() {
        ArrayList<HashKeyOnlyTestObject> found = Lists.newArrayList(repository.findAll());
        
        assertThat(found, containsInAnyOrder(loadedTestData.toArray()));
    }
    
    @Test
    public void testFindAllInIDList() {
        List<HashKeyOnlyTestObject> lookForItems = loadedTestData.stream().limit(2).collect(Collectors.toList());
        List<String> searchIds = lookForItems.stream().map(e -> e.getHashKey()).collect(Collectors.toList());
        
        ArrayList<HashKeyOnlyTestObject> found = Lists.newArrayList(repository.findAll(searchIds));

        assertThat(found, containsInAnyOrder(lookForItems.toArray()));
    }
    
    @Test
    public void testFindOne() {
        HashKeyOnlyTestObject expected = loadedTestData.get(0);
        
        HashKeyOnlyTestObject found = repository.findOne(expected.getHashKey());
        
        assertThat(found, is(expected));
    }
    
    @Test
    public void testSaveItem() {
        HashKeyOnlyTestObject created = createOne("Don Karnage", "Pirate");
        
        repository.save(created);
        
        assertThat(repository.findOne(created.getHashKey()), is(created));
    }

    @Test
    public void testSaveItems() {
        List<HashKeyOnlyTestObject> newItems = Arrays.asList(
                createOne("Don Karnage", "Pirate"),
                createOne("Mad Dog", "Pirate")
                );
                
        
        repository.save(newItems);
        
        Iterable<HashKeyOnlyTestObject> all = repository.findAll();
        
        List<HashKeyOnlyTestObject> expected = new ArrayList<>(loadedTestData);
        expected.addAll(newItems);
        
        assertThat(all, containsInAnyOrder(expected.toArray()));
    }
}
