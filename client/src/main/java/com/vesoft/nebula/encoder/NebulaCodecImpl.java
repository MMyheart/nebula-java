/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.encoder;

import com.google.common.collect.Lists;
import com.vesoft.nebula.Date;
import com.vesoft.nebula.DateTime;
import com.vesoft.nebula.Time;
import com.vesoft.nebula.Value;
import com.vesoft.nebula.meta.ColumnDef;
import com.vesoft.nebula.meta.ColumnTypeDef;
import com.vesoft.nebula.meta.EdgeItem;
import com.vesoft.nebula.meta.PropertyType;
import com.vesoft.nebula.meta.Schema;
import com.vesoft.nebula.meta.TagItem;

import javax.xml.crypto.Data;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

/**
 *  NebulaCodecImpl is an encoder to generate the given data.
 *  If the schema with default value, and the filed without given data, it will throw error.
 *  TODO: Support default value
 */
public class NebulaCodecImpl implements NebulaCodec {
    private static final int PARTITION_ID_SIZE = 4;
    private static final int TAG_ID_SIZE = 4;
    private static final int EDGE_TYPE_SIZE = 4;
    private static final int EDGE_RANKING_SIZE = 8;
    private static final int INDEX_ID_SIZE = 4;
    private static final int EDGE_VER_PLACE_HOLDER_SIZE = 1;
    private static final int VERTEX_SIZE = PARTITION_ID_SIZE + TAG_ID_SIZE;
    private static final int EDGE_SIZE = PARTITION_ID_SIZE + EDGE_TYPE_SIZE
        + EDGE_RANKING_SIZE + EDGE_VER_PLACE_HOLDER_SIZE;

    private static final int VERTEX_KEY_TYPE = 0x00000001;
    private static final int EDGE_KEY_TYPE = 0x00000002;
    private static final int INDEX_KEY_TYPE = 0x00000003;
    private static final int SEEK = 0xc70f6907;
    private final ByteOrder byteOrder;

    public NebulaCodecImpl() {
        this.byteOrder = ByteOrder.nativeOrder();
    }

    /**
     * @param vidLen the vidLen from the space description
     * @param partitionId the partitionId
     * @param vertexId the vertex id
     * @param tagId the tag id
     * @return
     */
    @Override
    public byte[] vertexKey(int vidLen,
                            int partitionId,
                            byte[] vertexId,
                            int tagId) {
        if (vertexId.length > vidLen) {
            throw new RuntimeException(
                "The length of vid size is out of the range, expected vidLen less then " + vidLen);
        }
        ByteBuffer buffer = ByteBuffer.allocate(VERTEX_SIZE + vidLen);
        buffer.order(this.byteOrder);
        partitionId = (partitionId << 8) | VERTEX_KEY_TYPE;
        buffer.putInt(partitionId)
            .put(vertexId);
        if (vertexId.length < vidLen) {
            ByteBuffer complementVid = ByteBuffer.allocate(vidLen - vertexId.length);
            Arrays.fill(complementVid.array(), (byte) '\0');
            buffer.put(complementVid);
        }
        buffer.putInt(tagId);
        return buffer.array();
    }

    /**
     * @param vidLen the vidLen from the space description
     * @param partitionId the partitionId
     * @param srcId the src id
     * @param edgeType the edge type
     * @param edgeRank the ranking
     * @param dstId the dstId
     * @return byte[]
     */
    @Override
    public byte[] edgeKeyByDefaultVer(int vidLen,
                                      int partitionId,
                                      byte[] srcId,
                                      int edgeType,
                                      long edgeRank,
                                      byte[] dstId) {
        return edgeKey(vidLen, partitionId, srcId, edgeType, edgeRank, dstId, (byte)1);
    }

    /**
     * @param vidLen the vidLen from the space description
     * @param partitionId the partitionId
     * @param srcId the src id
     * @param edgeType the edge type
     * @param edgeRank the ranking
     * @param dstId the dstId
     * @param edgeVerHolder the edgeVerHolder
     * @return byte[]
     */
    @Override
    public byte[] edgeKey(int vidLen,
                          int partitionId,
                          byte[] srcId,
                          int edgeType,
                          long edgeRank,
                          byte[] dstId,
                          byte edgeVerHolder) {
        if (srcId.length > vidLen || dstId.length > vidLen) {
            throw new RuntimeException(
                "The length of vid size is out of the range, expected vidLen less then " + vidLen);
        }
        ByteBuffer buffer = ByteBuffer.allocate(EDGE_SIZE + (vidLen << 1));
        buffer.order(this.byteOrder);
        partitionId = (partitionId << 8) | EDGE_KEY_TYPE;
        buffer.putInt(partitionId);
        buffer.put(srcId);
        if (srcId.length < vidLen) {
            ByteBuffer complementVid = ByteBuffer.allocate(vidLen - srcId.length);
            Arrays.fill(complementVid.array(), (byte) '\0');
            buffer.put(complementVid);
        }
        buffer.putInt(edgeType);
        buffer.put(encodeRank(edgeRank));
        buffer.put(dstId);
        if (dstId.length < vidLen) {
            ByteBuffer complementVid = ByteBuffer.allocate(vidLen - dstId.length);
            Arrays.fill(complementVid.array(), (byte) '\0');
            buffer.put(complementVid);
        }
        buffer.put(edgeVerHolder);
        return buffer.array();
    }

    /**
     * @param tag the TagItem
     * @param names the property names
     * @param values the property values
     * @return the encode byte[]
     * @throws RuntimeException expection
     */
    @Override
    public byte[] encodeTag(TagItem tag,
                            List<String> names,
                            List<Object> values) throws RuntimeException  {
        if (tag == null) {
            throw new RuntimeException("TagItem is null");
        }
        Schema schema = tag.getSchema();
        return encode(schema, tag.getVersion(), names, values);
    }

    /**
     * @param edge the EdgeItem
     * @param names the property names
     * @param values the property values
     * @return the encode byte[]
     * @throws RuntimeException expection
     */
    @Override
    public byte[] encodeEdge(EdgeItem edge,
                             List<String> names,
                             List<Object> values) throws RuntimeException  {
        if (edge == null) {
            throw new RuntimeException("EdgeItem is null");
        }
        Schema schema = edge.getSchema();
        return encode(schema, edge.getVersion(), names, values);
    }

    /**
     * @param schema the schema
     * @param ver the version of tag or edge
     * @param names the property names
     * @param values the property values
     * @return the encode byte[]
     * @throws RuntimeException expection
     */
    private byte[] encode(Schema schema,
                          long ver,
                          List<String> names,
                          List<Object> values)
        throws RuntimeException {
        if (names.size() != values.size()) {
            throw new RuntimeException(
                String.format("The names' size no equal with values' size, [%d] != [%d]",
                    names.size(), values.size()));
        }
        RowWriterImpl writer = new RowWriterImpl(genSchemaProvider(ver, schema), this.byteOrder);
        for (int i = 0; i < names.size(); i++) {
            writer.setValue(names.get(i), values.get(i));
        }
        writer.finish();
        return writer.encodeStr();
    }

    private SchemaProviderImpl genSchemaProvider(long ver, Schema schema) {
        SchemaProviderImpl schemaProvider = new SchemaProviderImpl(ver);
        for (ColumnDef col : schema.getColumns()) {
            ColumnTypeDef type = col.getType();
            boolean nullable = col.isSetNullable();
            boolean hasDefault = col.isSetDefault_value();
            int len = type.isSetType_length() ? type.getType_length() : 0;
            schemaProvider.addField(new String(col.getName()),
                type.type.getValue(),
                len,
                nullable,
                hasDefault ? col.getDefault_value() : null);
        }
        return schemaProvider;
    }

    private byte[] encodeRank(long rank) {
        long newRank = rank ^ (1L << 63);
        ByteBuffer rankBuf = ByteBuffer.allocate(Long.BYTES);
        rankBuf.order(ByteOrder.BIG_ENDIAN);
        rankBuf.putLong(newRank);
        return rankBuf.array();
    }

    @Override
    public byte[] tagIndexKey(int vidLen,
                              int partitionId,
                              int indexId,
                              byte[] vertexId,
                              Schema schema,
                              List<Object> values) {
        if (vertexId.length > vidLen) {
            throw new RuntimeException(
                    "The length of vid size is out of the range, expected vidLen less then "
                            + vidLen);
        }
        byte[] indexValue = encodeIndexValues(schema, values);
        ByteBuffer buffer = ByteBuffer.allocate(PARTITION_ID_SIZE + INDEX_ID_SIZE
                + vidLen + indexValue.length);
        buffer.order(this.byteOrder);
        partitionId = (partitionId << 8) | INDEX_KEY_TYPE;
        buffer.putInt(partitionId)
                .putInt(indexId)
                .put(indexValue)
                .put(vertexId);
        if (vertexId.length < vidLen) {
            ByteBuffer complementVid = ByteBuffer.allocate(vidLen - vertexId.length);
            Arrays.fill(complementVid.array(), (byte) '\0');
            buffer.put(complementVid);
        }
        return buffer.array();
    }

    @Override
    public byte[] edgeIndexKey(int vidLen,
                               int partitionId,
                               int indexId,
                               byte[] srcId,
                               long edgeRank,
                               byte[] dstId,
                               Schema schema,
                               List<Object> values) {
        if (srcId.length > vidLen || dstId.length > vidLen) {
            throw new RuntimeException(
                    "The length of vid size is out of the range, expected vidLen less then "
                            + vidLen);
        }
        byte[] indexValue = encodeIndexValues(schema, values);
        ByteBuffer buffer = ByteBuffer.allocate(PARTITION_ID_SIZE + INDEX_ID_SIZE
                + vidLen * 2 + EDGE_RANKING_SIZE + indexValue.length);
        buffer.order(this.byteOrder);
        partitionId = (partitionId << 8) | INDEX_KEY_TYPE;
        buffer.putInt(partitionId)
                .putInt(indexId)
                .put(indexValue)
                .put(srcId);
        if (srcId.length < vidLen) {
            ByteBuffer complementVid = ByteBuffer.allocate(vidLen - srcId.length);
            Arrays.fill(complementVid.array(), (byte) '\0');
            buffer.put(complementVid);
        }
        buffer.put(encodeRank(edgeRank))
                .put(dstId);
        if (dstId.length < vidLen) {
            ByteBuffer complementVid = ByteBuffer.allocate(vidLen - dstId.length);
            Arrays.fill(complementVid.array(), (byte) '\0');
            buffer.put(complementVid);
        }
        return buffer.array();
    }

    private byte[] encodeIndexValues(Schema schema,
                                     List<Object> values) {
        List<ColumnDef> columns = schema.getColumns();
        if (columns.size() != values.size()) {
            throw new RuntimeException(
                    "the number of index fields is " + columns.size() + ", not equal values "
                            + values.size());
        }
        boolean hasNullCol = false;
        byte[] nullableBitSet = new byte[]{0, 0};

        List<byte[]> bytes = Lists.newArrayList();
        int resultByteLen = 0;

        for (int i = 0; i < values.size(); i++) {
            if (columns.get(i).isSetNullable() && columns.get(i).isNullable()) {
                hasNullCol = true;
            }
            byte[] encodebytes = null;
            Object value = values.get(i);
            if (value instanceof Value) {
                if (((Value) value).getSetField() == Value.NVAL) {
                    nullableBitSet[i / 8] |= 0x80 >> (i % 8);
                    encodebytes = encodeNullValue(columns.get(i));
                } else {
                    encodebytes = encodeIndexValueByValue((Value) value, columns.get(i));
                }
            }  else {
                encodebytes = encodeIndexValueByObject(value, columns.get(i));
            }
            bytes.add(encodebytes);
            resultByteLen += encodebytes.length;
        }

        byte[] result = new byte[resultByteLen + (hasNullCol ? 2 : 0)];
        int position = 0;
        for (byte[] itemBytes : bytes) {
            for (byte itemByte : itemBytes) {
                result[position] = itemByte;
                position++;
            }
        }
        if (hasNullCol) {
            result[position] = nullableBitSet[0];
            result[position + 1] = nullableBitSet[1];
        }
        return result;
    }

    private byte[] encodeNullValue(ColumnDef columnDef) {
        int len = 0;
        switch (columnDef.getType().getType()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case TIMESTAMP:
                len = Long.BYTES;
                break;
            case FLOAT:
            case DOUBLE:
                len = Double.BYTES;
                break;
            case BOOL:
                len = Byte.BYTES;
                break;
            case STRING:
            case FIXED_STRING:
                len = columnDef.getType().getType_length();
                break;
            case TIME:
                len = Integer.BYTES + Byte.BYTES * 3;
                break;
            case DATE:
                len = Short.BYTES + Byte.BYTES * 2;
                break;
            case DATETIME:
                len = Integer.BYTES + Short.BYTES + Byte.BYTES * 5;
                break;
            default:
                throw new RuntimeException(
                        "column " + new String(columnDef.getName())
                                + " unsupported default value type in encode index key");
        }
        byte[] placeHolder = new byte[len];
        Arrays.fill(placeHolder, (byte)0xFF);
        return placeHolder;
    }

    private byte[] encodeIndexValueByValue(Value value, ColumnDef columnDef) {
        switch (value.getSetField()) {
            case Value.BVAL:
                return encodeIndexValueByObject(value.isBVal(), columnDef);
            case Value.IVAL:
                return encodeIndexValueByObject(value.getIVal(), columnDef);
            case Value.FVAL:
                return encodeIndexValueByObject(value.getFVal(), columnDef);
            case Value.SVAL:
                return encodeIndexValueByObject(new String(value.getSVal()), columnDef);
            case Value.DVAL:
                return encodeIndexValueByObject(value.getDVal(), columnDef);
            case Value.TVAL:
                return encodeIndexValueByObject(value.getTVal(), columnDef);
            case Value.DTVAL:
                return encodeIndexValueByObject(value.getDtVal(), columnDef);
            default:
                throw new RuntimeException(
                        "column " + new String(columnDef.getName())
                                + " unsupported default value type in encode index key");
        }
    }

    private byte[] encodeIndexValueByObject(Object value, ColumnDef columnDef) {
        if (columnDef.getType().getType() == PropertyType.FIXED_STRING && value instanceof String) {
            int fixLen = columnDef.getType().getType_length();
            String str = (String)value;
            int placeLen = fixLen - str.length();
            if (placeLen > 0) {
                char[] placeholder = new char[placeLen];
                Arrays.fill(placeholder, '\0');
                String placeHolderStr = new String(placeholder);
                return (str + placeHolderStr).getBytes();
            }
            return str.substring(0, fixLen).getBytes();
        }
        byte[] result = null;
        ByteBuffer byteBuffer = null;
        switch (columnDef.getType().getType()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                if (columnDef.getType().getType() == PropertyType.INT8) {
                    value = Byte.valueOf((byte)value).longValue();
                } else if (columnDef.getType().getType() == PropertyType.INT16) {
                    value = Short.valueOf((short)value).longValue();
                } else if (columnDef.getType().getType() == PropertyType.INT32) {
                    value = Integer.valueOf((int)value).longValue();
                }
                byteBuffer = ByteBuffer.allocate(Long.BYTES);
                byteBuffer.order(ByteOrder.BIG_ENDIAN);
                byteBuffer.putLong(((long) value) ^ (1L << 63));
                result = byteBuffer.array();
                break;
            case DOUBLE:
            case FLOAT:
                double doubleValue = (double)value;
                if (doubleValue < 0) {
                    long temp = -(Long.MAX_VALUE + Double.doubleToLongBits(doubleValue));
                    ByteBuffer tempBuf = ByteBuffer.allocate(Long.BYTES);
                    tempBuf.putLong(temp);
                    doubleValue = tempBuf.getDouble();
                }
                byteBuffer = ByteBuffer.allocate(Double.BYTES);
                byteBuffer.order(ByteOrder.BIG_ENDIAN);
                byteBuffer.putDouble(doubleValue);
                byte[] temp = byteBuffer.array();
                temp[0] ^= 0x80;
                result = temp;
                break;
            case UNKNOWN:
                break;
            case BOOL:
                boolean booleanValue = (boolean)value;
                byteBuffer = ByteBuffer.allocate(Byte.BYTES);
                byteBuffer.order(ByteOrder.BIG_ENDIAN);
                byteBuffer.put(booleanValue ? (byte)0x01 : (byte)0x00);
                result = byteBuffer.array();
                break;
            case STRING:
                String strValue = (String)value;
                result = strValue.getBytes();
                break;
            case TIME:
                Time time = (Time)value;
                byteBuffer = ByteBuffer.allocate(Integer.BYTES + Byte.BYTES * 3);
                byteBuffer.order(ByteOrder.BIG_ENDIAN);
                byteBuffer.put(time.getHour());
                byteBuffer.put(time.getMinute());
                byteBuffer.put(time.getSec());
                byteBuffer.putInt(time.getMicrosec());
                result = byteBuffer.array();
                break;
            case DATE:
                Date date = (Date)value;
                byteBuffer = ByteBuffer.allocate(Short.BYTES + Byte.BYTES * 2);
                byteBuffer.order(ByteOrder.BIG_ENDIAN);
                byteBuffer.putShort(date.getYear());
                byteBuffer.put(date.getMonth());
                byteBuffer.put(date.getDay());
                result = byteBuffer.array();
                break;
            case DATETIME:
                DateTime dateTime = (DateTime)value;
                byteBuffer = ByteBuffer.allocate(Integer.BYTES + Short.BYTES + Byte.BYTES * 5);
                byteBuffer.order(ByteOrder.BIG_ENDIAN);
                byteBuffer.putShort(dateTime.getYear());
                byteBuffer.put(dateTime.getMonth());
                byteBuffer.put(dateTime.getDay());
                byteBuffer.put(dateTime.getHour());
                byteBuffer.put(dateTime.getMinute());
                byteBuffer.put(dateTime.getSec());
                byteBuffer.putInt(dateTime.getMicrosec());
                result = byteBuffer.array();
                break;
            default:
                throw new RuntimeException(
                        "column " + new String(columnDef.getName())
                                + " unsupported default value type in encode index key");
        }
        return result;
    }
}
