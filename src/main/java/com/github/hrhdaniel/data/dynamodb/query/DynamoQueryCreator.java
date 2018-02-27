package com.github.hrhdaniel.data.dynamodb.query;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.repository.query.parser.PartTree;

import com.github.hrhdaniel.data.dynamodb.query.criteria.And;
import com.github.hrhdaniel.data.dynamodb.query.criteria.Criteria;
import com.github.hrhdaniel.data.dynamodb.query.criteria.FieldCriteria;
import com.github.hrhdaniel.data.dynamodb.query.criteria.HashKeyCriteria;
import com.github.hrhdaniel.data.dynamodb.query.criteria.Or;
import com.github.hrhdaniel.data.dynamodb.query.criteria.Query;
import com.github.hrhdaniel.data.dynamodb.query.criteria.RangeKeyCriteria;
import com.github.hrhdaniel.data.dynamodb.query.criteria.Scan;
import com.github.hrhdaniel.data.dynamodb.repository.DynamoGlobalSecondaryIndex;
import com.github.hrhdaniel.data.dynamodb.repository.DynamoRepositoryMetadata;

public class DynamoQueryCreator extends AbstractQueryCreator<DynamoSearch, Criteria> {

    private String hashKeyName;
    private HashKeyCriteria hashKey;

    private String rangeKeyName;
    private FieldCriteria rangeKey;

    private int criteriaCount = 0;
    private boolean isScan = false;
    private Set<String> scanReasons = new HashSet<>();

    private String indexName;

    public DynamoQueryCreator(Method method, DynamoRepositoryMetadata repoMeta, PartTree tree,
            ParameterAccessor parameters) {

        super(tree, parameters);

        DynamoGlobalSecondaryIndex indexAnnotation = method.getAnnotation(DynamoGlobalSecondaryIndex.class);

        if (indexAnnotation == null) {
            hashKeyName = repoMeta.getHashKeyPropertyName();
            rangeKeyName = repoMeta.getRangeKeyPropertyName();
        } else {
            indexName = indexAnnotation.indexName();
            hashKeyName = indexAnnotation.hashKeyName();
            rangeKeyName = indexAnnotation.rangeKeyName();
        }
    }

    @Override
    protected Criteria create(Part part, Iterator<Object> iterator) {
        String partName = part.getProperty().getSegment();
        if (partName.equals(hashKeyName)) {
            if (part.getType() != Type.SIMPLE_PROPERTY) {
                // Only equals is supported for hashkey on queries, so we have to scan
                isScan = true;
                scanReasons.add("HashKey is not equals condition");
            }
            if (hashKey != null) {
                // Found a second criteria for the HashKey. If it's exactly the same (which
                // could be logical in the case of an "OR") it would have to be the same as the
                // other instance found and we could still query... for now we'll just scan
                isScan = true;
                scanReasons.add("Multiple HashKey conditions found");
            }
            hashKey = new HashKeyCriteria(criteriaCount++, part, iterator);
            return hashKey;
        } else if (partName.equals(rangeKeyName)) {
            if (rangeKey != null) {
                // Just like above, we could check if these are equal to still query instead of
                // scan, but for now I'm lazy
                isScan = true;
                scanReasons.add("Multiple RangeKye conditions found");
            }
            rangeKey = new RangeKeyCriteria(criteriaCount++, part, iterator);
            return rangeKey;
        } else {
            return new FieldCriteria(criteriaCount++, part, iterator);
        }
    }

    @Override
    protected Criteria and(Part part, Criteria base, Iterator<Object> iterator) {
        return new And(base, create(part, iterator));
    }

    @Override
    protected Criteria or(Criteria base, Criteria criteria) {
        // As "And's" take precedence, an OR forces us to use a scan. Alternatively,
        // the hashkey and rangekey criteria would have to be the same and included on
        // both(all) sides of the OR, but we aren't implementing that ATM
        isScan = true;
        scanReasons.add("'OR' condition found");

        return new Or(base, criteria);
    }

    @Override
    protected DynamoSearch complete(Criteria criteria, Sort sort) {
        // TODO - Sorting
        if (hashKey == null) {
            isScan = true;
            scanReasons.add("No HashKey condition found");
        }

        if (isScan) {
            return new Scan(criteria, indexName, scanReasons);
        } else {
            Criteria keys = hashKey;
            if (rangeKey != null) {
                keys = new And(hashKey, rangeKey);
            }

            return new Query(keys, criteria, indexName);
        }
    }

}
