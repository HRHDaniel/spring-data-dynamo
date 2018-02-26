package com.github.hrhdaniel.data.dynamodb.query.criteria;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.data.repository.query.parser.Part;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.github.hrhdaniel.data.dynamodb.query.AWSAttributeValueHelper;

/**
 * Represents a filter criteria to apply to a single field (vs. a junction, such
 * as And or Or)
 * 
 * @author dharp
 */
public class FieldCriteria extends Criteria {

    private int id;
    private Part part;

    private String filterExpression;
    private Map<String, AttributeValue> attributeValues = new HashMap<>();
    private int varCount = 0;
    private String fieldPath;
    private Iterator<Object> iterator;
    private StringBuilder filter;

    public FieldCriteria(int id, Part part, Iterator<Object> iterator) {
        this.id = id;
        this.part = part;
        this.iterator = iterator;
        this.fieldPath = DynamoFieldNameMapper.mapToDynamoName(part.getProperty());
        filter = new StringBuilder();
        filterExpression = createFilter();
    }

    @Override
    public String getFilterExpression(boolean ignoreKeys) {
        return (ignoreKeys && isKey()) ? null : filterExpression;
    }

    @Override
    public Map<String, AttributeValue> getAttributeValues(boolean ignoreKeys) {
        return (ignoreKeys && isKey()) ? new HashMap<>() : attributeValues;
    }

    private String createFilter() {

        switch (part.getType()) {
        case SIMPLE_PROPERTY:
            addComparison(Operator.EQUAL);
            break;
        case NEGATING_SIMPLE_PROPERTY:
            addComparison(Operator.NOT_EQUAL);
            break;
        case GREATER_THAN:
        case AFTER:
            addComparison(Operator.GREATER_THAN);
            break;
        case GREATER_THAN_EQUAL:
            addComparison(Operator.GREATER_OR_EQUAL);
            break;
        case BEFORE:
        case LESS_THAN:
            addComparison(Operator.LESS_THAN);
            break;
        case LESS_THAN_EQUAL:
            addComparison(Operator.LESS_OR_EQUAL);
            break;
        case BETWEEN:
            addBetweenCondition();
            break;
        case CONTAINING:
            addContainsCondition();
            break;
        case FALSE:
            addComparison(false, Operator.EQUAL);
            break;
        case IN:
            // TODO ... iterator.next may return a list or array instead of using multiple
            // .next() ?
            addInCondition();
            break;
        case EXISTS:
        case IS_NOT_NULL:
            addIsNotNullCondition();
            break;
        case IS_NULL:
            addIsNullCondition();
            break;
        case NOT_CONTAINING:
            not(this::addContainsCondition);
            break;
        case NOT_IN:
            not(this::addInCondition);
            break;
        case STARTING_WITH:
            addStartingWithCondition();
            break;
        case TRUE:
            addComparison(true, Operator.EQUAL);
            break;
        default:
            // Unimplemented case statements: REGEX, LIKE, NOT_LIKE, NEAR, WITHIN, ENDING_WITH
            throw new UnsupportedOperationException("Expression type not supported: " + part.getType());
        }

        return filter.toString();
    }

    private void not(NoArgCondition condition) {
        filter.append("NOT ( ");
        condition.addCondition();
        filter.append(" ) ");
    }

    private void addContainsCondition() {
        addFilterMethod(AWSFilterMethod.CONTAINS, fieldPath, nextVariable());
    }

    private void addIsNullCondition() {
        addFilterMethod(AWSFilterMethod.NOT_EXISTS, fieldPath);
    }

    private void addIsNotNullCondition() {
        addFilterMethod(AWSFilterMethod.EXISTS, fieldPath);
    }

    private void addStartingWithCondition() {
        addFilterMethod(AWSFilterMethod.BEGINS_WITH, fieldPath, nextVariable());
    }

    private void addFilterMethod(AWSFilterMethod method, String... parms) {
        String parameters = String.join(", ", parms);
        filter.append(String.format("%s ( %s )", method, parameters));
    }

    private void addInCondition() {
        List<String> parameters = new ArrayList<>();
        for (int i = 1; i < part.getNumberOfArguments(); i++) {
            parameters.add(nextVariable());
        }

        filter.append(String.format("%s IN ( %s )", fieldPath, String.join(", ", parameters)));
    }

    private void addComparison(Operator comparison) {
        addComparison(iterator.next(), comparison);
    }

    private void addComparison(Object value, Operator comparison) {
        filter.append(fieldPath);
        filter.append(comparison);
        filter.append(nextVariable(value));
    }
    
    private void addBetweenCondition() {
        filter.append(String.format("%s BETWEEN %s AND %s", fieldPath, nextVariable(), nextVariable()));
    }

    private String nextVariable() {
        return nextVariable(iterator.next());
    }
    
    private String nextVariable(Object value) {
        // Since we're adding a bunch of variables to a larger context, we need to
        // generate unique names.
        // id scopes the variables to each criteria object. Variable count allows
        // multiple for each criteria (in case of things like "IN" criteria)
        String varName = ":c" + id + "v" + (varCount++);
        attributeValues.put(varName, AWSAttributeValueHelper.toAttributeValue(value));
        return varName;
    }

    private static interface NoArgCondition {
        void addCondition();
    }

    private enum AWSFilterMethod {
        NOT_EXISTS("attribute_not_exists"), BEGINS_WITH("begins_with"), EXISTS("attribute_exists"), CONTAINS(
                "contains");

        private String methodName;

        private AWSFilterMethod(String methodName) {
            this.methodName = methodName;
        }

        @Override
        public String toString() {
            return methodName;
        }
    }

    private enum Operator {
        EQUAL(" = "), GREATER_OR_EQUAL(" >= "), LESS_THAN(" < "), NOT_EQUAL(" <> "), GREATER_THAN(" > "), LESS_OR_EQUAL(
                " <= ");

        private String opStr;

        private Operator(String opStr) {
            this.opStr = opStr;

        }

        @Override
        public String toString() {
            return opStr;
        }
    }
}
