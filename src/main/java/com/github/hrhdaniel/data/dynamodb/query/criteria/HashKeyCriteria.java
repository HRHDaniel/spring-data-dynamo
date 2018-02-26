package com.github.hrhdaniel.data.dynamodb.query.criteria;

import java.util.Iterator;

import org.springframework.data.repository.query.parser.Part;

/**
 * Extension of a field criteria, but knows he's the hashkey for this search
 * 
 * @author dharp
 */
public class HashKeyCriteria extends FieldCriteria {

    public HashKeyCriteria(int id, Part part, Iterator<Object> values) {
        super(id, part, values);
    }

    @Override
    public boolean isKey() {
        return true;
    }
}
