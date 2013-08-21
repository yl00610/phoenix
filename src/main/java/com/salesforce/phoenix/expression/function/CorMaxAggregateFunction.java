package com.salesforce.phoenix.expression.function;

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.ColumnExpression;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.aggregator.Aggregator;
import com.salesforce.phoenix.expression.aggregator.CorMaxAggregator;
import com.salesforce.phoenix.expression.aggregator.CorMaxClientAggregator;
import com.salesforce.phoenix.parse.CorMaxAggregateParseNode;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.tuple.Tuple;

@BuiltInFunction(name = CorMaxAggregateFunction.NAME, nodeClass = CorMaxAggregateParseNode.class, args = {
        @Argument(), @Argument() })
public class CorMaxAggregateFunction extends DelegateConstantToCountAggregateFunction {

	public static final String NAME = "COR_MAX";

	public CorMaxAggregateFunction() {
	}

	public CorMaxAggregateFunction(List<Expression> childExpressions, CountAggregateFunction delegate) {
		super(childExpressions, delegate);
	}

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		boolean wasEvaluated = super.evaluate(tuple, ptr);
		if (!wasEvaluated) {
			return false;
		}
		if (isConstantExpression()) {
			getAggregatorExpression().evaluate(tuple, ptr);
		}

		return true;

	}

	@Override
	public ColumnModifier getColumnModifier() {
		// Our evaluate method maintains the same column modifier
		// that our argument had, since we're operating on the
		// bytes directly.
		return getChildren().get(0).getColumnModifier();
	}

	/**
	 * Determines whether or not the result of the function invocation will be
	 * ordered in the same way as the input to the function. Returning true
	 * enables an optimization to occur when a GROUP BY contains function
	 * invocations using the leading PK column(s).
	 * 
	 * @return true if the function invocation will preserve order for the
	 *         inputs versus the outputs and false otherwise
	 */
	@Override
	public boolean preservesOrder() {
		return false;
	}

	@Override
	public Aggregator newServerAggregator() {
		final ColumnExpression valueCol = (ColumnExpression) super.getChildren().get(0);
		final ColumnExpression sortCol =  (ColumnExpression) super.getChildren().get(1);
		ColumnModifier columnModifier = getAggregatorExpression().getColumnModifier();
		return new CorMaxAggregator(columnModifier, valueCol, sortCol);

	}

	public Aggregator newClientAggregator() {
		ColumnExpression valueCol = null;
		ColumnExpression sortCol = null;
		if (super.getChildren().size() == 2) {
			valueCol = (ColumnExpression) super.getChildren().get(0);
			sortCol = (ColumnExpression) super.getChildren().get(1);
		}
		ColumnModifier columnModifier = getAggregatorExpression().getColumnModifier();
		return new CorMaxClientAggregator(columnModifier, valueCol, sortCol);

	}

}
