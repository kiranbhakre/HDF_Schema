package com.hdp.registry;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.errors.DataException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;

//jackson

/**
 * Created by samgupta0 on 4/3/2018.
 */
/*
public class MyAvroData {


    public static final String NAMESPACE = "io.confluent.connect.avro";
    // Avro does not permit empty schema names, which might be the ideal default since we also are
    // not permitted to simply omit the name. Instead, make it very clear where the default is
    // coming from.
    public static final String DEFAULT_SCHEMA_NAME = "ConnectDefault";
    public static final String DEFAULT_SCHEMA_FULL_NAME = NAMESPACE + "." + DEFAULT_SCHEMA_NAME;
    public static final String MAP_ENTRY_TYPE_NAME = "MapEntry";
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";

    public static final String CONNECT_NAME_PROP = "connect.name";
    public static final String CONNECT_DOC_PROP = "connect.doc";
    public static final String CONNECT_RECORD_DOC_PROP = "connect.record.doc";
    public static final String CONNECT_ENUM_DOC_PROP = "connect.enum.doc";
    public static final String CONNECT_VERSION_PROP = "connect.version";
    public static final String CONNECT_DEFAULT_VALUE_PROP = "connect.default";
    public static final String CONNECT_PARAMETERS_PROP = "connect.parameters";

    public static final String CONNECT_TYPE_PROP = "connect.type";

    public static final String CONNECT_TYPE_INT8 = "int8";
    public static final String CONNECT_TYPE_INT16 = "int16";

    public static final String AVRO_TYPE_UNION = NAMESPACE + ".Union";
    public static final String AVRO_TYPE_ENUM = NAMESPACE + ".Enum";

    public static final String AVRO_TYPE_ANYTHING = NAMESPACE + ".Anything";

    private static final Map<String, Schema.Type> NON_AVRO_TYPES_BY_TYPE_CODE = new HashMap<>();

    static {
        NON_AVRO_TYPES_BY_TYPE_CODE.put(CONNECT_TYPE_INT8, Schema.Type.INT8);
        NON_AVRO_TYPES_BY_TYPE_CODE.put(CONNECT_TYPE_INT16, Schema.Type.INT16);
    }

    // Avro Java object types used by Connect schema types
    private static final Map<Schema.Type, List<Class>> SIMPLE_AVRO_SCHEMA_TYPES = new HashMap<>();

    static {
        SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.INT32, Arrays.asList((Class) Integer.class));
        SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.INT64, Arrays.asList((Class) Long.class));
        SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.FLOAT32, Arrays.asList((Class) Float.class));
        SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.FLOAT64, Arrays.asList((Class) Double.class));
        SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.BOOLEAN, Arrays.asList((Class) Boolean.class));
        SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.STRING, Arrays.asList((Class) CharSequence.class));
        SIMPLE_AVRO_SCHEMA_TYPES.put(
                Schema.Type.BYTES,
                Arrays.asList((Class) ByteBuffer.class, (Class) byte[].class, (Class) GenericFixed.class));
        SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.ARRAY, Arrays.asList((Class) Collection.class));
        SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.MAP, Arrays.asList((Class) Map.class));
    }

    private static final Map<Schema.Type, org.apache.avro.Schema.Type> CONNECT_TYPES_TO_AVRO_TYPES
            = new HashMap<>();

    static {
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.INT32, org.apache.avro.Schema.Type.INT);
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.INT64, org.apache.avro.Schema.Type.LONG);
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.FLOAT32, org.apache.avro.Schema.Type.FLOAT);
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.FLOAT64, org.apache.avro.Schema.Type.DOUBLE);
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.BOOLEAN, org.apache.avro.Schema.Type.BOOLEAN);
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.STRING, org.apache.avro.Schema.Type.STRING);
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.BYTES, org.apache.avro.Schema.Type.BYTES);
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.ARRAY, org.apache.avro.Schema.Type.ARRAY);
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.MAP, org.apache.avro.Schema.Type.MAP);
    }


    private static final String ANYTHING_SCHEMA_BOOLEAN_FIELD = "boolean";
    private static final String ANYTHING_SCHEMA_BYTES_FIELD = "bytes";
    private static final String ANYTHING_SCHEMA_DOUBLE_FIELD = "double";
    private static final String ANYTHING_SCHEMA_FLOAT_FIELD = "float";
    private static final String ANYTHING_SCHEMA_INT_FIELD = "int";
    private static final String ANYTHING_SCHEMA_LONG_FIELD = "long";
    private static final String ANYTHING_SCHEMA_STRING_FIELD = "string";
    private static final String ANYTHING_SCHEMA_ARRAY_FIELD = "array";
    private static final String ANYTHING_SCHEMA_MAP_FIELD = "map";

    public static final org.apache.avro.Schema ANYTHING_SCHEMA_MAP_ELEMENT;
    public static final org.apache.avro.Schema ANYTHING_SCHEMA;

    private static final org.apache.avro.Schema
            NULL_AVRO_SCHEMA =
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL);

    static {
        // Intuitively this should be a union schema. However, unions can't be named in Avro and this
        // is a self-referencing type, so we need to use a format in which we can name the entire schema

        ANYTHING_SCHEMA =
                org.apache.avro.SchemaBuilder.record(AVRO_TYPE_ANYTHING).namespace(NAMESPACE).fields()
                        .optionalBoolean(ANYTHING_SCHEMA_BOOLEAN_FIELD)
                        .optionalBytes(ANYTHING_SCHEMA_BYTES_FIELD)
                        .optionalDouble(ANYTHING_SCHEMA_DOUBLE_FIELD)
                        .optionalFloat(ANYTHING_SCHEMA_FLOAT_FIELD)
                        .optionalInt(ANYTHING_SCHEMA_INT_FIELD)
                        .optionalLong(ANYTHING_SCHEMA_LONG_FIELD)
                        .optionalString(ANYTHING_SCHEMA_STRING_FIELD)
                        .name(ANYTHING_SCHEMA_ARRAY_FIELD).type().optional().array()
                        .items().type(AVRO_TYPE_ANYTHING)
                        .name(ANYTHING_SCHEMA_MAP_FIELD).type().optional().array()
                        .items().record(MAP_ENTRY_TYPE_NAME).namespace(NAMESPACE).fields()
                        .name(KEY_FIELD).type(AVRO_TYPE_ANYTHING).noDefault()
                        .name(VALUE_FIELD).type(AVRO_TYPE_ANYTHING).noDefault()
                        .endRecord()
                        .endRecord();
        // This is convenient to have extracted; we can't define it before ANYTHING_SCHEMA because it
        // uses ANYTHING_SCHEMA in its definition.
        ANYTHING_SCHEMA_MAP_ELEMENT = ANYTHING_SCHEMA.getField("map").schema()
                .getTypes().get(1) // The "map" field is optional, get the schema from the union type
                .getElementType();
    }


    private Cache<Schema, org.apache.avro.Schema> fromConnectSchemaCache;
    private Cache<org.apache.avro.Schema, Schema> toConnectSchemaCache;
    private boolean connectMetaData;
    private boolean enhancedSchemaSupport;


    static final String AVRO_PROP = "avro";
    static final String AVRO_LOGICAL_TYPE_PROP = "logicalType";
    static final String AVRO_LOGICAL_TIMESTAMP_MILLIS = "timestamp-millis";
    static final String AVRO_LOGICAL_TIME_MILLIS = "time-millis";
    static final String AVRO_LOGICAL_DATE = "date";
    static final String AVRO_LOGICAL_DECIMAL = "decimal";
    static final String AVRO_LOGICAL_DECIMAL_SCALE_PROP = "scale";
    static final String AVRO_LOGICAL_DECIMAL_PRECISION_PROP = "precision";
    static final String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";
    static final Integer CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT = 64;

    private interface LogicalTypeConverter {

        Object convert(Schema schema, Object value);
    }
    private static final HashMap<String, LogicalTypeConverter> TO_AVRO_LOGICAL_CONVERTERS
            = new HashMap<>();

    static {
        TO_AVRO_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof BigDecimal)) {
                    throw new DataException(
                            "Invalid type for Decimal, expected BigDecimal but was " + value.getClass());
                }
                return Decimal.fromLogical(schema, (BigDecimal) value);
            }
        });

        TO_AVRO_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof java.util.Date)) {
                    throw new DataException(
                            "Invalid type for Date, expected Date but was " + value.getClass());
                }
                return Date.fromLogical(schema, (java.util.Date) value);
            }
        });

        TO_AVRO_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof java.util.Date)) {
                    throw new DataException(
                            "Invalid type for Time, expected Date but was " + value.getClass());
                }
                return Time.fromLogical(schema, (java.util.Date) value);
            }
        });

        TO_AVRO_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof java.util.Date)) {
                    throw new DataException(
                            "Invalid type for Timestamp, expected Date but was " + value.getClass());
                }
                return Timestamp.fromLogical(schema, (java.util.Date) value);
            }
        });
    }




    public Object fromConnectData(Schema schema, Object value) {
        org.apache.avro.Schema avroSchema = fromConnectSchema(schema);
        return fromConnectData(schema, avroSchema, value, true, false, enhancedSchemaSupport);
    }



    public org.apache.avro.Schema fromConnectSchema(Schema schema) {
        return fromConnectSchema(schema, new HashMap<Schema, org.apache.avro.Schema>());
    }


    public org.apache.avro.Schema fromConnectSchema(Schema schema,
                                                    Map<Schema, org.apache.avro.Schema> schemaMap) {
        return fromConnectSchema(schema, schemaMap, false);
    }

    public org.apache.avro.Schema fromConnectSchema(Schema schema,
                                                    Map<Schema, org.apache.avro.Schema> schemaMap,
                                                    boolean ignoreOptional) {
        if (schema == null) {
            return ANYTHING_SCHEMA;
        }
        org.apache.avro.Schema cached = fromConnectSchemaCache.get(schema);

        if (cached == null && !AVRO_TYPE_UNION.equals(schema.name()) && !schema.isOptional()) {
            cached = schemaMap.get(schema);
        }
        if (cached != null) {
            return cached;
        }

        String namespace = NAMESPACE;
        String name = DEFAULT_SCHEMA_NAME;
        if (schema.name() != null) {
            String[] split = splitName(schema.name());
            namespace = split[0];
            name = split[1];
        }

        // Extra type annotation information for otherwise lossy conversions
        String connectType = null;

        final org.apache.avro.Schema baseSchema;
        switch (schema.type()) {
            case INT8:
                connectType = CONNECT_TYPE_INT8;
                baseSchema = org.apache.avro.SchemaBuilder.builder().intType();
                break;
            case INT16:
                connectType = CONNECT_TYPE_INT16;
                baseSchema = org.apache.avro.SchemaBuilder.builder().intType();
                break;
            case INT32:
                baseSchema = org.apache.avro.SchemaBuilder.builder().intType();
                break;
            case INT64:
                baseSchema = org.apache.avro.SchemaBuilder.builder().longType();
                break;
            case FLOAT32:
                baseSchema = org.apache.avro.SchemaBuilder.builder().floatType();
                break;
            case FLOAT64:
                baseSchema = org.apache.avro.SchemaBuilder.builder().doubleType();
                break;
            case BOOLEAN:
                baseSchema = org.apache.avro.SchemaBuilder.builder().booleanType();
                break;
            case STRING:
                if (enhancedSchemaSupport && schema.parameters() != null
                        && schema.parameters().containsKey(AVRO_TYPE_ENUM)) {
                    List<String> symbols = new ArrayList<>();
                    for (Map.Entry<String, String> entry : schema.parameters().entrySet()) {
                        if (entry.getKey().startsWith(AVRO_TYPE_ENUM + ".")) {
                            symbols.add(entry.getValue());
                        }
                    }
                    baseSchema =
                            org.apache.avro.SchemaBuilder.builder().enumeration(
                                    schema.parameters().get(AVRO_TYPE_ENUM))
                                    .doc(schema.parameters().get(CONNECT_ENUM_DOC_PROP))
                                    .symbols(symbols.toArray(new String[symbols.size()]));
                } else {
                    baseSchema = org.apache.avro.SchemaBuilder.builder().stringType();
                }
                break;
            case BYTES:
                baseSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
                if (Decimal.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
                    int scale = Integer.parseInt(schema.parameters().get(Decimal.SCALE_FIELD));
                    baseSchema.addProp(AVRO_LOGICAL_DECIMAL_SCALE_PROP, new IntNode(scale));
                    if (schema.parameters().containsKey(CONNECT_AVRO_DECIMAL_PRECISION_PROP)) {
                        String precisionValue = schema.parameters().get(CONNECT_AVRO_DECIMAL_PRECISION_PROP);
                        int precision = Integer.parseInt(precisionValue);
                        baseSchema.addProp(AVRO_LOGICAL_DECIMAL_PRECISION_PROP, new IntNode(precision));
                    } else {
                        baseSchema
                                .addProp(AVRO_LOGICAL_DECIMAL_PRECISION_PROP,
                                        new IntNode(CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT));
                    }
                }
                break;
            case ARRAY:
                baseSchema = org.apache.avro.SchemaBuilder.builder().array()
                        .items(fromConnectSchema(schema.valueSchema()));
                break;
            case MAP:
                // Avro only supports string keys, so we match the representation when possible, but
                // otherwise fall back on a record representation
                if (schema.keySchema().type() == Schema.Type.STRING && !schema.keySchema().isOptional()) {
                    baseSchema = org.apache.avro.SchemaBuilder.builder()
                            .map().values(fromConnectSchema(schema.valueSchema()));
                } else {
                    // Special record name indicates format
                    org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fieldAssembler
                            = org.apache.avro.SchemaBuilder.builder()
                            .array().items()
                            .record(MAP_ENTRY_TYPE_NAME).namespace(NAMESPACE).fields();
                    addAvroRecordField(fieldAssembler, KEY_FIELD, schema.keySchema(), schemaMap);
                    addAvroRecordField(fieldAssembler, VALUE_FIELD, schema.valueSchema(), schemaMap);
                    baseSchema = fieldAssembler.endRecord();
                }
                break;
            case STRUCT:
                if (AVRO_TYPE_UNION.equals(schema.name())) {
                    List<org.apache.avro.Schema> unionSchemas = new ArrayList<>();
                    if (schema.isOptional()) {
                        unionSchemas.add(org.apache.avro.SchemaBuilder.builder().nullType());
                    }
                    for (Field field : schema.fields()) {
                        unionSchemas.add(fromConnectSchema(nonOptional(field.schema()), schemaMap, true));
                    }
                    baseSchema = org.apache.avro.Schema.createUnion(unionSchemas);
                } else if (schema.isOptional()) {
                    List<org.apache.avro.Schema> unionSchemas = new ArrayList<>();
                    unionSchemas.add(org.apache.avro.SchemaBuilder.builder().nullType());
                    unionSchemas.add(fromConnectSchema(nonOptional(schema), schemaMap, false));
                    baseSchema = org.apache.avro.Schema.createUnion(unionSchemas);
                } else {
                    String doc = schema.parameters() != null
                            ? schema.parameters().get(CONNECT_RECORD_DOC_PROP)
                            : null;
                    org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema>
                            fieldAssembler =
                            org.apache.avro.SchemaBuilder.record(name != null ? name : DEFAULT_SCHEMA_NAME)
                                    .namespace(namespace)
                                    .doc(doc).fields();
                    for (Field field : schema.fields()) {
                        addAvroRecordField(fieldAssembler, field.name(), field.schema(), schemaMap);
                    }
                    baseSchema = fieldAssembler.endRecord();
                }
                break;
            default:
                throw new DataException("Unknown schema type: " + schema.type());
        }

        org.apache.avro.Schema finalSchema = baseSchema;
        if (!baseSchema.getType().equals(org.apache.avro.Schema.Type.UNION)) {
            if (connectMetaData) {
                if (schema.doc() != null) {
                    baseSchema.addProp(CONNECT_DOC_PROP, schema.doc());
                }
                if (schema.version() != null) {
                    baseSchema.addProp(CONNECT_VERSION_PROP,
                            JsonNodeFactory.instance.numberNode(schema.version()));
                }
                if (schema.parameters() != null) {
                    baseSchema.addProp(CONNECT_PARAMETERS_PROP, parametersFromConnect(schema.parameters()));
                }
                if (schema.defaultValue() != null) {
                    baseSchema.addProp(CONNECT_DEFAULT_VALUE_PROP,
                            defaultValueFromConnect(schema, schema.defaultValue()));
                }
                if (schema.name() != null) {
                    baseSchema.addProp(CONNECT_NAME_PROP, schema.name());
                }
                // Some Connect types need special annotations to preserve the types accurate due to
                // limitations in Avro. These types get an extra annotation with their Connect type
                if (connectType != null) {
                    baseSchema.addProp(CONNECT_TYPE_PROP, connectType);
                }
            }

            // Only Avro named types (record, enum, fixed) may contain namespace + name. Only Connect's
            // struct converts to one of those (record), so for everything else that has a name we store
            // the full name into a special property. For uniformity, we also duplicate this info into
            // the same field in records as well even though it will also be available in the namespace()
            // and name().
            if (schema.name() != null) {
                if (Decimal.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
                    baseSchema.addProp(AVRO_LOGICAL_TYPE_PROP, AVRO_LOGICAL_DECIMAL);
                } else if (Time.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
                    baseSchema.addProp(AVRO_LOGICAL_TYPE_PROP, AVRO_LOGICAL_TIME_MILLIS);
                } else if (Timestamp.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
                    baseSchema.addProp(AVRO_LOGICAL_TYPE_PROP, AVRO_LOGICAL_TIMESTAMP_MILLIS);
                } else if (Date.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
                    baseSchema.addProp(AVRO_LOGICAL_TYPE_PROP, AVRO_LOGICAL_DATE);
                }
            }

            if (schema.parameters() != null) {
                for (Map.Entry<String, String> entry : schema.parameters().entrySet()) {
                    if (entry.getKey().startsWith(AVRO_PROP)) {
                        baseSchema.addProp(entry.getKey(), entry.getValue());
                    }
                }
            }

            // Note that all metadata has already been processed and placed on the baseSchema because we
            // can't store any metadata on the actual top-level schema when it's a union because of Avro
            // constraints on the format of schemas.
            if (!ignoreOptional) {
                if (schema.isOptional()) {
                    if (schema.defaultValue() != null) {
                        finalSchema = org.apache.avro.SchemaBuilder.builder().unionOf()
                                .type(baseSchema).and()
                                .nullType()
                                .endUnion();
                    } else {
                        finalSchema = org.apache.avro.SchemaBuilder.builder().unionOf()
                                .nullType().and()
                                .type(baseSchema)
                                .endUnion();
                    }
                }
            }
        }

        if (!schema.isOptional()) {
            schemaMap.put(schema, finalSchema);
        }
        fromConnectSchemaCache.put(schema, finalSchema);
        return finalSchema;
    }



        private static String[] splitName(String fullName) {
        String[] result = new String[2];
        int indexLastDot = fullName.lastIndexOf('.');
        if (indexLastDot >= 0) {
            result[0] = fullName.substring(0, indexLastDot);
            result[1] = fullName.substring(indexLastDot + 1);
        } else {
            result[0] = null;
            result[1] = fullName;
        }
        return result;
    }

    private void addAvroRecordField(
            org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fieldAssembler,
            String fieldName, Schema fieldSchema, Map<Schema, org.apache.avro.Schema> schemaMap) {
        org.apache.avro.SchemaBuilder.GenericDefault<org.apache.avro.Schema> fieldAvroSchema
                = fieldAssembler.name(fieldName).doc(fieldSchema.doc()).type(fromConnectSchema(fieldSchema,
                schemaMap));
        if (fieldSchema.defaultValue() != null) {
            fieldAvroSchema.withDefault(defaultValueFromConnect(fieldSchema, fieldSchema.defaultValue()));
        } else {
            if (fieldSchema.isOptional()) {
                fieldAvroSchema.withDefault(JsonNodeFactory.instance.nullNode());
            } else {
                fieldAvroSchema.noDefault();
            }
        }
    }

    public static Schema nonOptional(Schema schema) {
        return new ConnectSchema(schema.type(), false, schema.defaultValue(), schema.name(),
                schema.version(), schema.doc(),
                schema.parameters(),
                fields(schema),
                keySchema(schema),
                valueSchema(schema));
    }

    public static List<Field> fields(Schema schema) {
        Schema.Type type = schema.type();
        if (Schema.Type.STRUCT.equals(type)) {
            return schema.fields();
        } else {
            return null;
        }
    }

    public static Schema keySchema(Schema schema) {
        Schema.Type type = schema.type();
        if (Schema.Type.MAP.equals(type)) {
            return schema.keySchema();
        } else {
            return null;
        }
    }

    public static Schema valueSchema(Schema schema) {
        Schema.Type type = schema.type();
        if (Schema.Type.MAP.equals(type) || Schema.Type.ARRAY.equals(type)) {
            return schema.valueSchema();
        } else {
            return null;
        }
    }


    private static JsonNode defaultValueFromConnect(Schema schema, Object value) {
        try {
            // If this is a logical type, convert it from the convenient Java type to the underlying
            // serializeable format
            Object defaultVal = value;
            if (schema != null && schema.name() != null) {
                LogicalTypeConverter logicalConverter = TO_AVRO_LOGICAL_CONVERTERS.get(schema.name());
                if (logicalConverter != null && value != null) {
                    defaultVal = logicalConverter.convert(schema, value);
                }
            }

            switch (schema.type()) {
                case INT8:
                    return JsonNodeFactory.instance.numberNode((Byte) defaultVal);
                case INT16:
                    return JsonNodeFactory.instance.numberNode((Short) defaultVal);
                case INT32:
                    return JsonNodeFactory.instance.numberNode((Integer) defaultVal);
                case INT64:
                    return JsonNodeFactory.instance.numberNode((Long) defaultVal);
                case FLOAT32:
                    return JsonNodeFactory.instance.numberNode((Float) defaultVal);
                case FLOAT64:
                    return JsonNodeFactory.instance.numberNode((Double) defaultVal);
                case BOOLEAN:
                    return JsonNodeFactory.instance.booleanNode((Boolean) defaultVal);
                case STRING:
                    return JsonNodeFactory.instance.textNode((String) defaultVal);
                case BYTES:
                    if (defaultVal instanceof byte[]) {
                        return JsonNodeFactory.instance.binaryNode((byte[]) defaultVal);
                    } else {
                        return JsonNodeFactory.instance.binaryNode(((ByteBuffer) defaultVal).array());
                    }
                case ARRAY: {
                    ArrayNode array = JsonNodeFactory.instance.arrayNode();
                    for (Object elem : (Collection<Object>) defaultVal) {
                        array.add(defaultValueFromConnect(schema.valueSchema(), elem));
                    }
                    return array;
                }
                case MAP:
                    if (schema.keySchema().type() == Schema.Type.STRING && !schema.keySchema().isOptional()) {
                        ObjectNode node = JsonNodeFactory.instance.objectNode();
                        for (Map.Entry<String, Object> entry : ((Map<String, Object>) defaultVal).entrySet()) {
                            JsonNode entryDef = defaultValueFromConnect(schema.valueSchema(), entry.getValue());
                            node.put(entry.getKey(), entryDef);
                        }
                        return node;
                    } else {
                        ArrayNode array = JsonNodeFactory.instance.arrayNode();
                        for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) defaultVal).entrySet()) {
                            JsonNode keyDefault = defaultValueFromConnect(schema.keySchema(), entry.getKey());
                            JsonNode valDefault = defaultValueFromConnect(schema.valueSchema(), entry.getValue());
                            ArrayNode jsonEntry = JsonNodeFactory.instance.arrayNode();
                            jsonEntry.add(keyDefault);
                            jsonEntry.add(valDefault);
                            array.add(jsonEntry);
                        }
                        return array;
                    }
                case STRUCT: {
                    ObjectNode node = JsonNodeFactory.instance.objectNode();
                    Struct struct = ((Struct) defaultVal);
                    for (Field field : (schema.fields())) {
                        JsonNode fieldDef = defaultValueFromConnect(field.schema(), struct.get(field));
                        node.put(field.name(), fieldDef);
                    }
                    return node;
                }
                default:
                    throw new DataException("Unknown schema type:" + schema.type());
            }
        } catch (ClassCastException e) {
            throw new DataException("Invalid type used for default value of "
                    + schema.type()
                    + " field: "
                    + schema.defaultValue().getClass());
        }
    }

    private static JsonNode parametersFromConnect(Map<String, String> params) {
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }














    private static Schema.Type schemaTypeForSchemalessJavaType(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Byte) {
            return Schema.Type.INT8;
        } else if (value instanceof Short) {
            return Schema.Type.INT16;
        } else if (value instanceof Integer) {
            return Schema.Type.INT32;
        } else if (value instanceof Long) {
            return Schema.Type.INT64;
        } else if (value instanceof Float) {
            return Schema.Type.FLOAT32;
        } else if (value instanceof Double) {
            return Schema.Type.FLOAT64;
        } else if (value instanceof Boolean) {
            return Schema.Type.BOOLEAN;
        } else if (value instanceof String) {
            return Schema.Type.STRING;
        } else if (value instanceof Collection) {
            return Schema.Type.ARRAY;
        } else if (value instanceof Map) {
            return Schema.Type.MAP;
        } else {
            throw new DataException("Unknown Java type for schemaless data: " + value.getClass());
        }
    }






    private static void validateSchemaValue(Schema schema, Object value) throws DataException {
        if (value == null && schema != null && !schema.isOptional()) {
            throw new DataException("Found null value for non-optional schema");
        }
    }


    private static Object maybeAddContainer(org.apache.avro.Schema avroSchema, Object value,
                                            boolean wrap) {
        return wrap ? new NonRecordContainer(avroSchema, value) : value;
    }


    private static Object maybeWrapSchemaless(Schema schema, Object value, String typeField) {
        if (schema != null) {
            return value;
        }

        GenericRecordBuilder builder = new GenericRecordBuilder(ANYTHING_SCHEMA);
        if (value != null) {
            builder.set(typeField, value);
        }
        return builder.build();
    }


    private static org.apache.avro.Schema avroSchemaForUnderlyingTypeIfOptional(
            Schema schema, org.apache.avro.Schema avroSchema) {

        if (schema != null && schema.isOptional()) {
            if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
                for (org.apache.avro.Schema typeSchema : avroSchema
                        .getTypes()) {
                    if (!typeSchema.getType().equals(org.apache.avro.Schema.Type.NULL)
                            && (typeSchema.getFullName().equals(schema.name())
                            || typeSchema.getType().getName().equals(schema.type().getName()))) {
                        return typeSchema;
                    }
                }
            } else {
                throw new DataException(
                        "An optinal schema should have an Avro Union type, not "
                                + schema.type());
            }
        }
        return avroSchema;
    }


    */
/**
     * MapEntry types in connect Schemas are represented as Arrays of record.
     * Return the array type from the union instead of the union itself.
     *//*

    private static org.apache.avro.Schema avroSchemaForUnderlyingMapEntryType(
            Schema schema,
            org.apache.avro.Schema avroSchema) {

        if (schema != null && schema.isOptional()) {
            if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
                for (org.apache.avro.Schema typeSchema : avroSchema.getTypes()) {
                    if (!typeSchema.getType().equals(org.apache.avro.Schema.Type.NULL)
                            && Schema.Type.ARRAY.getName().equals(typeSchema.getType().getName())) {
                        return typeSchema;
                    }
                }
            } else {
                throw new DataException(
                        "An optional schema should have an Avro Union type, not "
                                + schema.type());
            }
        }
        return avroSchema;
    }






    private static Object fromConnectData(
            Schema schema, org.apache.avro.Schema avroSchema,
            Object logicalValue, boolean requireContainer,
            boolean requireSchemalessContainerNull, boolean enhancedSchemaSupport
    ) {
        Schema.Type schemaType = schema != null
                ? schema.type()
                : schemaTypeForSchemalessJavaType(logicalValue);
        if (schemaType == null) {
            // Schemaless null data since schema is null and we got a null schema type from the value
            if (requireSchemalessContainerNull) {
                return new GenericRecordBuilder(ANYTHING_SCHEMA).build();
            } else {
                return null;
            }
        }

        validateSchemaValue(schema, logicalValue);

        if (logicalValue == null) {
            // But if this is schemaless, we may not be able to return null directly
            if (schema == null && requireSchemalessContainerNull) {
                return new GenericRecordBuilder(ANYTHING_SCHEMA).build();
            } else {
                return null;
            }
        }

        // If this is a logical type, convert it from the convenient Java type to the underlying
        // serializeable format
        Object value = logicalValue;
        if (schema != null && schema.name() != null) {
            LogicalTypeConverter logicalConverter = TO_AVRO_LOGICAL_CONVERTERS.get(schema.name());
            if (logicalConverter != null && logicalValue != null) {
                value = logicalConverter.convert(schema, logicalValue);
            }
        }

        try {
            switch (schemaType) {
                case INT8: {
                    Byte byteValue = (Byte) value; // Check for correct type
                    Integer convertedByteValue = byteValue == null ? null : byteValue.intValue();
                    return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, convertedByteValue, ANYTHING_SCHEMA_INT_FIELD),
                            requireContainer);
                }
                case INT16: {
                    Short shortValue = (Short) value; // Check for correct type
                    Integer convertedShortValue = shortValue == null ? null : shortValue.intValue();
                    return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, convertedShortValue, ANYTHING_SCHEMA_INT_FIELD),
                            requireContainer);
                }

                case INT32:
                    Integer intValue = (Integer) value; // Check for correct type
                    return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_INT_FIELD),
                            requireContainer);
                case INT64:
                    Long longValue = (Long) value; // Check for correct type
                    return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_LONG_FIELD),
                            requireContainer);
                case FLOAT32:
                    Float floatValue = (Float) value; // Check for correct type
                    return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_FLOAT_FIELD),
                            requireContainer);
                case FLOAT64:
                    Double doubleValue = (Double) value; // Check for correct type
                    return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_DOUBLE_FIELD),
                            requireContainer);
                case BOOLEAN:
                    Boolean boolValue = (Boolean) value; // Check for correct type
                    return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_BOOLEAN_FIELD),
                            requireContainer);
                case STRING:
                    if (enhancedSchemaSupport && schema != null && schema.parameters() != null
                            && schema.parameters().containsKey(AVRO_TYPE_ENUM)) {
                        String enumSchemaName = schema.parameters().get(AVRO_TYPE_ENUM);
                        org.apache.avro.Schema enumSchema;
                        if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
                            int enumIndex = avroSchema.getIndexNamed(enumSchemaName);
                            enumSchema = avroSchema.getTypes().get(enumIndex);
                        } else {
                            enumSchema = avroSchema;
                        }
                        value = new GenericData.EnumSymbol(enumSchema, (String) value);
                    } else {
                        String stringValue = (String) value; // Check for correct type
                    }
                    return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_STRING_FIELD),
                            requireContainer);

                case BYTES: {
                    ByteBuffer bytesValue = value instanceof byte[] ? ByteBuffer.wrap((byte[]) value) :
                            (ByteBuffer) value;
                    return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, bytesValue, ANYTHING_SCHEMA_BYTES_FIELD),
                            requireContainer);
                }

                case ARRAY: {
                    Collection<Object> list = (Collection<Object>) value;
                    // TODO most types don't need a new converted object since types pass through
                    List<Object> converted = new ArrayList<>(list.size());
                    Schema elementSchema = schema != null ? schema.valueSchema() : null;
                    org.apache.avro.Schema underlyingAvroSchema = avroSchemaForUnderlyingTypeIfOptional(
                            schema, avroSchema);
                    org.apache.avro.Schema elementAvroSchema =
                            schema != null ? underlyingAvroSchema.getElementType() : ANYTHING_SCHEMA;
                    for (Object val : list) {
                        converted.add(
                                fromConnectData(
                                        elementSchema,
                                        elementAvroSchema,
                                        val,
                                        false,
                                        true,
                                        enhancedSchemaSupport
                                )
                        );
                    }
                    return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, converted, ANYTHING_SCHEMA_ARRAY_FIELD),
                            requireContainer);
                }

                case MAP: {
                    Map<Object, Object> map = (Map<Object, Object>) value;
                    org.apache.avro.Schema underlyingAvroSchema;
                    if (schema != null && schema.keySchema().type() == Schema.Type.STRING
                            && !schema.keySchema().isOptional()) {

                        // TODO most types don't need a new converted object since types pass through
                        underlyingAvroSchema = avroSchemaForUnderlyingTypeIfOptional(schema, avroSchema);
                        Map<String, Object> converted = new HashMap<>();
                        for (Map.Entry<Object, Object> entry : map.entrySet()) {
                            // Key is a String, no conversion needed
                            Object convertedValue = fromConnectData(schema.valueSchema(),
                                    underlyingAvroSchema.getValueType(),
                                    entry.getValue(), false, true, enhancedSchemaSupport
                            );
                            converted.put((String) entry.getKey(), convertedValue);
                        }
                        return maybeAddContainer(avroSchema, converted, requireContainer);
                    } else {
                        List<GenericRecord> converted = new ArrayList<>(map.size());
                        underlyingAvroSchema = avroSchemaForUnderlyingMapEntryType(schema, avroSchema);
                        org.apache.avro.Schema elementSchema =
                                schema != null
                                        ? underlyingAvroSchema.getElementType()
                                        : ANYTHING_SCHEMA_MAP_ELEMENT;
                        org.apache.avro.Schema avroKeySchema = elementSchema.getField(KEY_FIELD).schema();
                        org.apache.avro.Schema avroValueSchema = elementSchema.getField(VALUE_FIELD).schema();
                        for (Map.Entry<Object, Object> entry : map.entrySet()) {
                            Object keyConverted = fromConnectData(schema != null ? schema.keySchema() : null,
                                    avroKeySchema, entry.getKey(), false, true,
                                    enhancedSchemaSupport);
                            Object valueConverted = fromConnectData(schema != null ? schema.valueSchema() : null,
                                    avroValueSchema, entry.getValue(), false,
                                    true, enhancedSchemaSupport);
                            converted.add(
                                    new GenericRecordBuilder(elementSchema)
                                            .set(KEY_FIELD, keyConverted)
                                            .set(VALUE_FIELD, valueConverted)
                                            .build()
                            );
                        }
                        return maybeAddContainer(
                                avroSchema, maybeWrapSchemaless(schema, converted, ANYTHING_SCHEMA_MAP_FIELD),
                                requireContainer);
                    }
                }

                case STRUCT: {
                    Struct struct = (Struct) value;
                    if (!struct.schema().equals(schema)) {
                        throw new DataException("Mismatching struct schema");
                    }
                    //This handles the inverting of a union which is held as a struct, where each field is
                    // one of the union types.
                    if (AVRO_TYPE_UNION.equals(schema.name())) {
                        for (Field field : schema.fields()) {
                            Object object = struct.get(field);
                            if (object != null) {
                                return fromConnectData(
                                        field.schema(),
                                        avroSchema,
                                        object,
                                        false,
                                        true,
                                        enhancedSchemaSupport
                                );
                            }
                        }
                        return fromConnectData(schema, avroSchema, null, false, true, enhancedSchemaSupport);
                    } else {
                        org.apache.avro.Schema underlyingAvroSchema = avroSchemaForUnderlyingTypeIfOptional(
                                schema, avroSchema);
                        GenericRecordBuilder convertedBuilder = new GenericRecordBuilder(underlyingAvroSchema);
                        for (Field field : schema.fields()) {
                            org.apache.avro.Schema.Field theField = underlyingAvroSchema.getField(field.name());
                            org.apache.avro.Schema fieldAvroSchema = theField.schema();
                            convertedBuilder.set(
                                    field.name(),
                                    fromConnectData(field.schema(), fieldAvroSchema, struct.get(field), false,
                                            true, enhancedSchemaSupport)
                            );
                        }
                        return convertedBuilder.build();
                    }
                }

                default:
                    throw new DataException("Unknown schema type: " + schema.type());
            }
        } catch (ClassCastException e) {
            throw new DataException("Invalid type for " + schema.type() + ": " + value.getClass());
        }
    }
}
*/
