package com.github.hrhdaniel.data.dynamodb;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsNot;
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
import com.github.hrhdaniel.data.dynamodb.DynamicRepoMethodsIT.TestConfig;
import com.github.hrhdaniel.data.dynamodb.config.EnableDynamoRepositories;
import com.github.hrhdaniel.data.dynamodb.domain.CompositeKeyObject;
import com.github.hrhdaniel.data.dynamodb.domain.NestedObject;
import com.github.hrhdaniel.data.dynamodb.repositories.CompositeKeyRepository;

import testutils.SpringInitializationListener;
import testutils.SpringWrappedTestRunner;

@RunWith(SpringWrappedTestRunner.class)
@ContextConfiguration(classes = { TestConfig.class })
@SpringBootTest
public class DynamicRepoMethodsIT implements SpringInitializationListener {

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
         DeleteTableRequest deleteTableRequest =
         mapper.generateDeleteTableRequest(CompositeKeyObject.class);
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

    private CompositeKeyObject createOne(String name, int size, String gender, String detail, boolean isBear,
            String... extraStrings) {
        CompositeKeyObject entity = new CompositeKeyObject();
        entity.setObjectName(name);
        entity.setObjectSize(size);
        entity.setGender(gender);
        entity.setBear(isBear);
        NestedObject nested = new NestedObject();
        nested.setSubData(detail);
        entity.setNested(nested);
        entity.setExtraStrings(Arrays.asList(extraStrings));
        return entity;
    }

    @Autowired
    private AmazonDynamoDB client;

    @Autowired
    private DynamoDBMapper mapper;

    @Autowired
    private CompositeKeyRepository repository;

    private List<CompositeKeyObject> loadedTestData = Arrays.asList(
            createOne("Kit Cloudkicker", 1, "M", "Air surfing", true),
            createOne("Baloo", 2, "M", "Pilot", true),
            createOne("Molly Cunningham", 1, "F", "Button Nose", true),
            createOne("Wildcat", 3, "M", "mechanic", false),
            createOne("King Louie", 3, "M", null, false, "stout", "orange", "orangutan"));

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
    public void testNestedScan() {
        List<CompositeKeyObject> items = repository.findByNestedSubData("Pilot");

        assertThat(items.size(), is(1));
        assertThat(items.get(0).getObjectName(), is("Baloo"));
    }

    @Test
    public void testNot() {
        // When
        List<CompositeKeyObject> items = repository.findByObjectNameIsNot("Baloo");

        // Then
        int expectedCount = (int) loadedTestData.stream().filter(o -> !"Baloo".equals(o.getObjectName())).count();
        assertThat(items.size(), is(expectedCount));
        assertThat(items.get(0).getObjectName(), IsNot.not("Baloo"));
        assertThat(items.get(1).getObjectName(), IsNot.not("Baloo"));
    }

    @Test
    public void testStartsWith() {
        List<CompositeKeyObject> items = repository.findByObjectNameStartsWith("Molly");

        assertThat(items.size(), is(1));
        assertThat(items.get(0).getObjectName(), is("Molly Cunningham"));
    }

    @Test
    public void testAndLessThan() {
        List<CompositeKeyObject> items = repository.findByGenderAndObjectSizeLessThan("M", 2);

        assertThat(items.size(), is(1));
        assertThat(items.get(0).getObjectName(), is("Kit Cloudkicker"));
    }

    @Test
    public void testIsFalse() {
        List<CompositeKeyObject> items = repository.findByBearIsFalse();

        assertThat(items.size(), is(2));
        List<String> resultNames = items.stream().map(CompositeKeyObject::getObjectName).collect(Collectors.toList());
        assertThat(resultNames, containsInAnyOrder("Wildcat", "King Louie"));
        assertThat(items.get(0).getObjectName(), is("Wildcat"));
    }

    @Test
    public void testIsTrue() {
        List<CompositeKeyObject> items = repository.findByBearIsTrue();

        assertThat(items.size(), is(3));
        List<String> names = items.stream().map(o -> o.getObjectName()).collect(Collectors.toList());
        assertThat(names, not(contains(("Wildcat"))));
    }

    @Test
    public void testInList() {
        List<String> searchFor = Arrays.asList("Molly Cunningham", "Kit Cloudkicker");

        List<CompositeKeyObject> results = repository.findByObjectNameIn(searchFor);

        assertThat(results.size(), is(2));
        List<String> resultNames = results.stream().map(CompositeKeyObject::getObjectName).collect(Collectors.toList());
        assertThat(resultNames, containsInAnyOrder("Molly Cunningham", "Kit Cloudkicker"));
    }

    @Test
    public void testInArray() {
        String[] searchFor = new String[] { "Molly Cunningham", "Kit Cloudkicker" };

        List<CompositeKeyObject> results = repository.findByObjectNameIn(searchFor);

        assertThat(results.size(), is(2));
        List<String> resultNames = results.stream().map(CompositeKeyObject::getObjectName).collect(Collectors.toList());
        assertThat(resultNames, containsInAnyOrder("Molly Cunningham", "Kit Cloudkicker"));
    }

    @Test
    public void testOr() {
        List<CompositeKeyObject> results = repository.findByObjectNameOrObjectName("Molly Cunningham",
                "Kit Cloudkicker");

        assertThat(results.size(), is(2));
        List<String> resultNames = results.stream().map(CompositeKeyObject::getObjectName).collect(Collectors.toList());
        assertThat(resultNames, containsInAnyOrder("Molly Cunningham", "Kit Cloudkicker"));
    }

    @Test
    public void testBetween() {
        List<CompositeKeyObject> results = repository.findByObjectSizeBetween(1, 2);

        assertThat(results.size(), is(3));
        List<String> resultNames = results.stream().map(CompositeKeyObject::getObjectName).collect(Collectors.toList());
        assertThat(resultNames, containsInAnyOrder("Baloo", "Molly Cunningham", "Kit Cloudkicker"));
    }

    @Test
    public void testGreaterThan() {
        List<CompositeKeyObject> results = repository.findByObjectSizeGreaterThan(2);

        assertThat(results.size(), is(2));
        List<String> resultNames = results.stream().map(CompositeKeyObject::getObjectName).collect(Collectors.toList());
        assertThat(resultNames, containsInAnyOrder("Wildcat", "King Louie"));
    }

    @Test
    public void testGreaterThanEquals() {
        List<CompositeKeyObject> results = repository.findByObjectSizeGreaterThanEqual(2);

        assertThat(results.size(), is(3));
        List<String> resultNames = results.stream().map(CompositeKeyObject::getObjectName).collect(Collectors.toList());
        assertThat(resultNames, containsInAnyOrder("Wildcat", "Baloo", "King Louie"));
    }

    @Test
    public void testLessThan() {
        List<CompositeKeyObject> results = repository.findByObjectSizeLessThan(2);

        assertThat(results.size(), is(2));
        List<String> resultNames = results.stream().map(CompositeKeyObject::getObjectName).collect(Collectors.toList());
        assertThat(resultNames, containsInAnyOrder("Molly Cunningham", "Kit Cloudkicker"));
    }

    @Test
    public void testLessThanEquals() {
        List<CompositeKeyObject> results = repository.findByObjectSizeLessThanEqual(2);

        assertThat(results.size(), is(3));
        List<String> resultNames = results.stream().map(CompositeKeyObject::getObjectName).collect(Collectors.toList());
        assertThat(resultNames, containsInAnyOrder("Baloo", "Molly Cunningham", "Kit Cloudkicker"));
    }

    @Test
    public void testExists() {
        List<CompositeKeyObject> results = repository.findByNestedSubDataExists();

        assertThat(results.size(), is(4));
        List<String> resultNames = results.stream().map(CompositeKeyObject::getObjectName).collect(Collectors.toList());
        assertThat(resultNames, not(hasItem("King Louie")));
    }

    @Test
    public void testIsNull() {
        List<CompositeKeyObject> results = repository.findByNestedSubDataIsNull();

        assertThat(results.size(), is(1));
        List<String> resultNames = results.stream().map(CompositeKeyObject::getObjectName).collect(Collectors.toList());
        assertThat(resultNames, hasItem("King Louie"));
    }

    @Test
    public void testNotIn() {
        List<String> searchFor = Arrays.asList("Molly Cunningham", "Kit Cloudkicker");
        int expectedResults = loadedTestData.size() - 2;

        List<CompositeKeyObject> results = repository.findByObjectNameNotIn(searchFor);

        assertThat(results.size(), is(expectedResults));
        List<String> resultNames = results.stream().map(CompositeKeyObject::getObjectName).collect(Collectors.toList());
        assertThat(resultNames, not(hasItems("Molly Cunningham", "Kit Cloudkicker")));
    }

    @Test
    public void testContains() {
        List<CompositeKeyObject> results = repository.findByExtraStringsContains("orange");

        assertThat(results.size(), is(1));
        List<String> resultNames = results.stream().map(CompositeKeyObject::getObjectName).collect(Collectors.toList());
        assertThat(resultNames, hasItem("King Louie"));
    }
    
    @Test
    public void testNotContains() {
        int expectedResults = loadedTestData.size() - 1;

        List<CompositeKeyObject> results = repository.findByExtraStringsNotContains("orange");

        assertThat(results.size(), is(expectedResults));
        List<String> resultNames = results.stream().map(CompositeKeyObject::getObjectName).collect(Collectors.toList());
        assertThat(resultNames, not(hasItem("King Louie")));
    }
}
