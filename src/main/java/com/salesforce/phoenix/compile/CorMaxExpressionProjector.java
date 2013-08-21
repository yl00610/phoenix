package com.salesforce.phoenix.compile;

import java.sql.SQLException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.aggregator.CorMaxValue;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;

public class CorMaxExpressionProjector extends ExpressionProjector {

	public CorMaxExpressionProjector(String name, String tableName, Expression expression, boolean isCaseSensitive) {
	    super(name, tableName, expression, isCaseSensitive);
    }

    @Override
    public final Object getValue(Tuple tuple, PDataType type, ImmutableBytesWritable ptr) throws SQLException {
        Expression expression = getExpression();
        if (!expression.evaluate(tuple, ptr)) {
            return null;
        }
        if (ptr.getLength() == 0) {
            return null;
        }        
        CorMaxValue currentRow = new CorMaxValue(null, null, ptr);
        return type.toObject(currentRow.getValue(), expression.getDataType(), expression.getColumnModifier());
    }
}
