package com.github.hrhdaniel.data.dynamodb.query.criteria;

/**
 * Creates an OR criteria
 * 
 * @author dharp
 */
public class Or extends Junction {

    public Or(Criteria left, Criteria right) {
        super(left, right);
    }

    @Override
    protected String getJunctionStr() {
        return " OR ";
    }

}
