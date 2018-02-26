package com.github.hrhdaniel.data.dynamodb.query.criteria;

/**
 * Combines two Criteria
 * 
 * @author dharp
 */
public class And extends Junction {

    public And(Criteria left, Criteria right) {
        super(left, right);
    }

    @Override
    protected String getJunctionStr() {
        return " AND ";
    }

}
