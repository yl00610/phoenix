package com.salesforce.phoenix.expression.aggregator;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.expression.ColumnExpression;
import com.salesforce.phoenix.expression.KeyValueColumnExpression;
import com.salesforce.phoenix.expression.RowKeyColumnExpression;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.ByteUtil;

public class CorMaxValue implements Comparable<CorMaxValue> {
	private final ImmutableBytesWritable value = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
	private final ImmutableBytesWritable sort = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);

	private ColumnExpression valueCol;
	private ColumnExpression sortCol;

	public CorMaxValue(ColumnExpression valueCol, ColumnExpression sortCol) {
		this.valueCol = valueCol;
		this.sortCol = sortCol;
	}

	public CorMaxValue(ColumnExpression valueCol, ColumnExpression sortCol, ImmutableBytesWritable value, ImmutableBytesWritable sort) {
		this(valueCol, sortCol);
		this.value.set(value.get(), value.getOffset(), value.getLength());
		this.sort.set(sort.get(), sort.getOffset(), sort.getLength());
	}
	
	public CorMaxValue(ImmutableBytesWritable value, ImmutableBytesWritable sort) {
		this(null, null, value, sort);
	}

	public CorMaxValue(ColumnExpression valueCol, ColumnExpression sortCol, ImmutableBytesWritable combinedBytes) {
		this(valueCol, sortCol);
		setCombinedBytes(combinedBytes);
	}

	public CorMaxValue(ImmutableBytesWritable combinedBytes) {
		this(null, null, combinedBytes);
	}

	public void update(CorMaxValue value) {
		this.value.set(value.getValue().get(), value.getValue().getOffset(), value.getValue().getLength());
		this.sort.set(value.getSort().get(), value.getSort().getOffset(), value.getSort().getLength());
	}

	/**
	 * 
	 * @return byte array = lengthOfValue + value + sort
	 */
	public ImmutableBytesWritable getCombinedBytes() {
		byte[] valueLen = Bytes.toBytes(this.value.getLength());
		ImmutableBytesWritable valueLenWritable = new ImmutableBytesWritable(valueLen);
		return new ImmutableBytesWritable(ByteUtil.concat(null, valueLenWritable, value, sort));
	}

	/**
	 * Split byte[] to lengthOfValue, value and sort
	 * 
	 * @param combinedBytes
	 */
	public void setCombinedBytes(ImmutableBytesWritable combinedBytes) {
		int length = combinedBytes.getLength();
		int offset = combinedBytes.getOffset();
		int valueOffset =  offset + Bytes.SIZEOF_INT;
		int valueLength = Bytes.toInt(combinedBytes.get(), combinedBytes.getOffset(), Bytes.SIZEOF_INT);
		int sortOffset = valueOffset + valueLength;
		int sortLength = length - valueLength - Bytes.SIZEOF_INT;
		this.value.set(combinedBytes.get(), valueOffset, valueLength);
		this.sort.set(combinedBytes.get(), sortOffset, sortLength);
	}

	public void reset() {
		sort.set(ByteUtil.EMPTY_BYTE_ARRAY);
		value.set(ByteUtil.EMPTY_BYTE_ARRAY);
	}

	public boolean isNull() {
		return sort.equals(ByteUtil.EMPTY_BYTE_ARRAY) || value.equals(ByteUtil.EMPTY_BYTE_ARRAY);
	}

	@Override
	public int compareTo(CorMaxValue value) {
		if (this.sort.equals(ByteUtil.EMPTY_BYTE_ARRAY)) {
			return -1;
		}
		if (value == null || value.getSort().equals(ByteUtil.EMPTY_BYTE_ARRAY)) {
			return 1;
		}
		return getSortDataType().compareTo(this.sort, value.getSort());
	}

	@Override
	public String toString() {
		return "value:" + getValueDataType().toObject(value) + ", sort:" + getSortDataType().toObject(sort);

	}

	public ColumnExpression getValueCol() {
		return valueCol;
	}

	public ColumnExpression getSortCol() {
		return sortCol;
	}

	public ImmutableBytesWritable getValue() {
		return value;
	}

	public byte[] getValueBytes() {
		return this.value.get();
	}

	public int getValueOffset() {
		return this.value.getOffset();
	}

	public int getValueLength() {
		return this.value.getLength();
	}

	public ImmutableBytesWritable getSort() {
		return sort;
	}

	protected PDataType getSortDataType() {
		return sortCol.getDataType();
	}

	protected PDataType getValueDataType() {
		return valueCol.getDataType();
	}

	public byte[] getValueColumnFamily() {
		return getColumnFamily(valueCol);
	}

	public byte[] getValueColumnName() {
		return getColumnName(valueCol);
	}

	public byte[] getSortColumnFamily() {
		return getColumnFamily(sortCol);
	}

	public byte[] getSortColumnName() {
		return getColumnName(sortCol);
	}

	public boolean isValueRowKey() {
		return isRowKey(valueCol);
	}

	public boolean isSortRowKey() {
		return isRowKey(sortCol);
	}

	private byte[] getColumnName(ColumnExpression column) {
		if (column != null && column instanceof KeyValueColumnExpression) {
			return ((KeyValueColumnExpression) column).getColumnName();
		}
		return null;
	}

	private byte[] getColumnFamily(ColumnExpression column) {
		if (column != null && column instanceof KeyValueColumnExpression) {
			return ((KeyValueColumnExpression) column).getColumnFamily();
		}
		return null;
	}

	private boolean isRowKey(ColumnExpression column) {
		return column != null && column instanceof RowKeyColumnExpression;
	}

}
