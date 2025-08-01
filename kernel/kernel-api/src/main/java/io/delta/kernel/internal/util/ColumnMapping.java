/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.internal.util;

import static io.delta.kernel.internal.DeltaErrors.columnNotFoundInSchema;
import static io.delta.kernel.internal.data.TransactionStateRow.getColumnMappingMode;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Collections.singletonMap;

import io.delta.kernel.data.Row;
import io.delta.kernel.exceptions.InvalidConfigurationValueException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.icebergcompat.IcebergCompatMetadataValidatorAndUpdater;
import io.delta.kernel.types.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** Utilities related to the column mapping feature. */
public class ColumnMapping {
  private ColumnMapping() {}

  public enum ColumnMappingMode {
    NONE("none"),
    ID("id"),
    NAME("name");

    public final String value;

    ColumnMappingMode(String value) {
      this.value = value;
    }

    public static ColumnMappingMode fromTableConfig(String modeString) {
      for (ColumnMappingMode mode : ColumnMappingMode.values()) {
        if (mode.value.equalsIgnoreCase(modeString)) {
          return mode;
        }
      }
      throw new InvalidConfigurationValueException(
          COLUMN_MAPPING_MODE_KEY,
          modeString,
          String.format("Needs to be one of: %s.", Arrays.toString(ColumnMappingMode.values())));
    }

    @Override
    public String toString() {
      return this.value;
    }
  }

  public static final String COLUMN_MAPPING_MODE_KEY = "delta.columnMapping.mode";
  public static final String COLUMN_MAPPING_PHYSICAL_NAME_KEY = "delta.columnMapping.physicalName";
  public static final String COLUMN_MAPPING_ID_KEY = "delta.columnMapping.id";
  public static final String COLUMN_MAPPING_NESTED_IDS_KEY = "delta.columnMapping.nested.ids";

  public static final String PARQUET_FIELD_ID_KEY = "parquet.field.id";
  public static final String PARQUET_FIELD_NESTED_IDS_METADATA_KEY = "parquet.field.nested.ids";
  public static final String COLUMN_MAPPING_MAX_COLUMN_ID_KEY = "delta.columnMapping.maxColumnId";

  /////////////////
  // Public APIs //
  /////////////////

  /**
   * Returns the column mapping mode from the given configuration.
   *
   * @param configuration Configuration
   * @return Column mapping mode. One of ("none", "name", "id")
   */
  public static ColumnMappingMode getColumnMappingMode(Map<String, String> configuration) {
    return Optional.ofNullable(configuration.get(COLUMN_MAPPING_MODE_KEY))
        .map(ColumnMappingMode::fromTableConfig)
        .orElse(ColumnMappingMode.NONE);
  }

  /**
   * Helper method that converts the logical schema (requested by the connector) to physical schema
   * of the data stored in data files based on the table's column mapping mode. Field-id column
   * metadata is preserved when cmMode = ID, all column metadata is otherwise removed.
   *
   * <p>We require {@code fullSchema} in addition to the pruned schema we want to convert since we
   * need the complete field metadata as it is stored in the schema in the _delta_log. We cannot be
   * sure (and do not enforce) that this metadata is preserved by the connector.
   *
   * @param prunedSchema the logical read schema requested by the connector
   * @param fullSchema the full delta schema (with complete metadata) as read from the _delta_log
   * @param columnMappingMode Column mapping mode
   */
  public static StructType convertToPhysicalSchema(
      StructType prunedSchema, StructType fullSchema, ColumnMappingMode columnMappingMode) {
    switch (columnMappingMode) {
      case NONE:
        return prunedSchema;
      case ID: // fall through
      case NAME:
        boolean includeFieldIds = columnMappingMode == ColumnMappingMode.ID;
        return convertToPhysicalSchema(prunedSchema, fullSchema, includeFieldIds);
      default:
        throw new UnsupportedOperationException(
            "Unsupported column mapping mode: " + columnMappingMode);
    }
  }

  /**
   * Converts a logical column to a physical column based on the table's column mapping mode. The
   * field-id metadata is preserved when cmMode = ID, all column metadata is otherwise removed.
   *
   * <p>We require {@code fullSchema} in addition to the logical field we want to convert since we
   * need the complete field metadata as it is stored in the schema in the _delta_log. We cannot be
   * sure (and do not enforce) that this metadata is preserved by the connector.
   *
   * @param logicalField the logical read column requested by the connector
   * @param fullSchema the full delta schema (with complete metadata) as read from the _delta_log
   * @param columnMappingMode Column mapping mode
   */
  public static StructField convertToPhysicalColumn(
      StructField logicalField, StructType fullSchema, ColumnMappingMode columnMappingMode) {
    switch (columnMappingMode) {
      case NONE:
        return logicalField;
      case ID: // fall through
      case NAME:
        boolean includeFieldIds = columnMappingMode == ColumnMappingMode.ID;
        return convertToPhysicalColumn(logicalField, fullSchema, includeFieldIds);
      default:
        throw new UnsupportedOperationException(
            "Unsupported column mapping mode: " + columnMappingMode);
    }
  }

  /** Returns the physical name for a given {@link StructField} */
  public static String getPhysicalName(StructField field) {
    if (hasPhysicalName(field)) {
      return field.getMetadata().getString(COLUMN_MAPPING_PHYSICAL_NAME_KEY);
    } else {
      return field.getName();
    }
  }

  /** Returns the column id for a given {@link StructField} */
  public static int getColumnId(StructField field) {
    checkArgument(
        field.getMetadata().contains(COLUMN_MAPPING_ID_KEY),
        "Field does not have column id set in it's metadata");
    return field.getMetadata().getLong(COLUMN_MAPPING_ID_KEY).intValue();
  }

  public static void verifyColumnMappingChange(
      Map<String, String> oldConfig, Map<String, String> newConfig, boolean isNewTable) {
    ColumnMappingMode oldMappingMode = getColumnMappingMode(oldConfig);
    ColumnMappingMode newMappingMode = getColumnMappingMode(newConfig);

    checkArgument(
        isNewTable || validModeChange(oldMappingMode, newMappingMode),
        "Changing column mapping mode from '%s' to '%s' is not supported",
        oldMappingMode,
        newMappingMode);
  }

  public static boolean isColumnMappingModeEnabled(ColumnMappingMode columnMappingMode) {
    return columnMappingMode == ColumnMappingMode.ID || columnMappingMode == ColumnMappingMode.NAME;
  }

  /**
   * Updates the column mapping metadata if needed based on the column mapping mode and whether the
   * icebergCompatV2 is enabled. If column mapping/iceberg compat info is already present in the
   * metadata, this method does nothing and returns an empty Optional. Callers can avoid updating
   * the metadata if the metadata has not changed.
   *
   * @param metadata Current metadata.
   * @param isNewTable Whether this is part of a commit that sets the mapping mode on a new table.
   * @return Optional of the updated metadata if it has changed, Optional.empty() otherwise.
   */
  public static Optional<Metadata> updateColumnMappingMetadataIfNeeded(
      Metadata metadata, boolean isNewTable) {
    ColumnMappingMode columnMappingMode = getColumnMappingMode(metadata.getConfiguration());
    switch (columnMappingMode) {
      case NONE:
        return Optional.empty();
      case ID: // fall through
      case NAME:
        return assignColumnIdAndPhysicalName(metadata, isNewTable);
      default:
        throw new UnsupportedOperationException(
            "Unsupported column mapping mode: " + columnMappingMode);
    }
  }

  /** Returns the physical column and data type for a given logical column based on the schema. */
  public static Tuple2<Column, DataType> getPhysicalColumnNameAndDataType(
      StructType schema, Column logicalColumn) {
    List<String> physicalNameParts = new ArrayList<>();
    DataType currentType = schema;

    // Traverse through each level of the logical name to resolve its corresponding physical name.
    for (String namePart : logicalColumn.getNames()) {
      if (!(currentType instanceof StructType)) {
        throw columnNotFoundInSchema(logicalColumn, schema);
      }

      StructType structType = (StructType) currentType;
      // Find the field in the current structure that matches the given name
      StructField field =
          structType.fields().stream()
              .filter(f -> f.getName().equalsIgnoreCase(namePart))
              .findFirst()
              .orElseThrow(() -> columnNotFoundInSchema(logicalColumn, schema));
      physicalNameParts.add(ColumnMapping.getPhysicalName(field));
      currentType = field.getDataType();
    }
    return new Tuple2<>(new Column(physicalNameParts.toArray(new String[0])), currentType);
  }

  /**
   * Utility method to block writing into a table with column mapping enabled. Currently Kernel only
   * supports the metadata updates on tables with column mapping enabled. Data writes into such
   * tables using the data transformation APIs provided by the Kernel are not supported yet.
   */
  public static void blockIfColumnMappingEnabled(Row transactionState) {
    ColumnMapping.ColumnMappingMode columnMappingMode =
        TransactionStateRow.getColumnMappingMode(transactionState);
    if (columnMappingMode != ColumnMapping.ColumnMappingMode.NONE) {
      throw new UnsupportedOperationException(
          "Writing into column mapping enabled table is not supported yet.");
    }
  }

  ////////////////////////////
  // Private Helper Methods //
  ////////////////////////////

  /** Visible for testing */
  static int findMaxColumnId(StructType schema) {
    return new SchemaIterable(schema)
        .stream()
            .mapToInt(
                e -> {
                  int columnId = hasColumnId(e.getField()) ? getColumnId(e.getField()) : 0;
                  int nestedMaxId =
                      hasNestedColumnIds(e.getField()) ? getMaxNestedColumnId(e.getField()) : 0;
                  return Math.max(columnId, nestedMaxId);
                })
            .max()
            .orElse(0);
  }

  static boolean hasColumnId(StructField field) {
    return field.getMetadata().contains(COLUMN_MAPPING_ID_KEY);
  }

  static boolean hasPhysicalName(StructField field) {
    return field.getMetadata().contains(COLUMN_MAPPING_PHYSICAL_NAME_KEY);
  }

  /**
   * Utility method to convert the given logical schema to physical schema, recursively converting
   * sub-types in case of complex types. When {@code includeFieldId} is true, converted physical
   * schema will have field ids in the metadata. Column metadata is otherwise removed.
   */
  private static StructType convertToPhysicalSchema(
      StructType prunedSchema, StructType fullSchema, boolean includeFieldId) {
    StructType newSchema = new StructType();
    for (StructField prunedField : prunedSchema.fields()) {
      newSchema = newSchema.add(convertToPhysicalColumn(prunedField, fullSchema, includeFieldId));
    }
    return newSchema;
  }

  /**
   * Utility method to convert the given logical field to a physical field, recursively converting
   * sub-types in case of complex types. When {@code includeFieldId} is true, converted physical
   * schema will have field ids in the metadata. Column metadata is otherwise removed.
   */
  private static StructField convertToPhysicalColumn(
      StructField logicalField, StructType fullSchema, boolean includeFieldId) {
    StructField completeField = fullSchema.get(logicalField.getName());
    DataType physicalType =
        convertToPhysicalType(
            logicalField.getDataType(), completeField.getDataType(), includeFieldId);
    String physicalName = completeField.getMetadata().getString(COLUMN_MAPPING_PHYSICAL_NAME_KEY);

    if (!includeFieldId) {
      return new StructField(physicalName, physicalType, logicalField.isNullable());
    }

    Long fieldId = completeField.getMetadata().getLong(COLUMN_MAPPING_ID_KEY);
    FieldMetadata.Builder builder = FieldMetadata.builder().putLong(PARQUET_FIELD_ID_KEY, fieldId);

    // convertToPhysicalSchema(..) gets called when trying to find the read schema
    // for the Parquet reader. This currently assumes that if the nested field IDs for
    // the 'element' and 'key'/'value' fields of Arrays/Maps haven been written,
    // then IcebergCompatV2 is enabled because the schema we are looking at is from
    // the DeltaLog and has nested field IDs setup
    if (hasNestedColumnIds(completeField)) {
      builder.putFieldMetadata(
          PARQUET_FIELD_NESTED_IDS_METADATA_KEY, getNestedColumnIds(completeField));
    }

    return new StructField(physicalName, physicalType, logicalField.isNullable(), builder.build());
  }

  private static DataType convertToPhysicalType(
      DataType logicalType, DataType physicalType, boolean includeFieldId) {
    if (logicalType instanceof StructType) {
      return convertToPhysicalSchema(
          (StructType) logicalType, (StructType) physicalType, includeFieldId);
    } else if (logicalType instanceof ArrayType) {
      ArrayType logicalArrayType = (ArrayType) logicalType;
      return new ArrayType(
          convertToPhysicalType(
              logicalArrayType.getElementType(),
              ((ArrayType) physicalType).getElementType(),
              includeFieldId),
          logicalArrayType.containsNull());
    } else if (logicalType instanceof MapType) {
      MapType logicalMapType = (MapType) logicalType;
      MapType physicalMapType = (MapType) physicalType;
      return new MapType(
          convertToPhysicalType(
              logicalMapType.getKeyType(), physicalMapType.getKeyType(), includeFieldId),
          convertToPhysicalType(
              logicalMapType.getValueType(), physicalMapType.getValueType(), includeFieldId),
          logicalMapType.isValueContainsNull());
    }
    return logicalType;
  }

  private static boolean validModeChange(ColumnMappingMode oldMode, ColumnMappingMode newMode) {
    // only upgrade from none to name mapping is allowed
    return oldMode.equals(newMode)
        || (oldMode == ColumnMappingMode.NONE && newMode == ColumnMappingMode.NAME);
  }

  /**
   * For each column/field in a {@link Metadata}'s schema, assign an id using the current maximum id
   * as the basis and increment from there. Additionally, assign a physical name based on a random
   * UUID or re-use the old display name if the mapping mode is updated on an existing table. When
   * `icebergWriterCompatV1` is enabled, we assign physical names as 'col-[colId]'.
   *
   * @param metadata The new metadata to assign ids and physical names to
   * @param isNewTable whether this is part of a commit that sets the mapping mode on a new table
   * @return Optional {@link Metadata} with a new schema where ids and physical names have been
   *     assigned if the schema has changed, returns Optional.empty() otherwise
   */
  private static Optional<Metadata> assignColumnIdAndPhysicalName(
      Metadata metadata, boolean isNewTable) {
    StructType oldSchema = metadata.getSchema();

    // When icebergWriterCompatV1 or icebergWriterCompatV3 is enabled we require
    // physicalName='col-[columnId]'
    boolean useColumnIdForPhysicalName =
        TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.fromMetadata(metadata)
            || TableConfig.ICEBERG_WRITER_COMPAT_V3_ENABLED.fromMetadata(metadata);

    // This is the maxColumnId to use when assigning any new field-ids; we update this as we
    // traverse the schema and after traversal this is the value that should be stored in the
    // metadata. Note - this could be greater than the current value stored in the metadata if
    // the connector has added new fields with field-ids
    AtomicInteger maxColumnId =
        new AtomicInteger(
            Math.max(
                Integer.parseInt(
                    metadata
                        .getConfiguration()
                        .getOrDefault(COLUMN_MAPPING_MAX_COLUMN_ID_KEY, "0")),
                findMaxColumnId(oldSchema)));

    StructType newSchema = new StructType();
    for (StructField field : oldSchema.fields()) {
      newSchema =
          newSchema.add(
              transformAndAssignColumnIdAndPhysicalName(
                  assignColumnIdAndPhysicalNameToField(
                      field, maxColumnId, isNewTable, useColumnIdForPhysicalName),
                  maxColumnId,
                  isNewTable,
                  useColumnIdForPhysicalName));
    }

    if (IcebergCompatMetadataValidatorAndUpdater.isIcebergCompatEnabled(metadata)) {
      newSchema = rewriteFieldIdsForIceberg(newSchema, maxColumnId);
    }

    // The maxColumnId in the metadata may be out-of-date either due to field-id assignment
    // performed in this function, or due to connector adding new fields
    boolean shouldUpdateMaxId =
        TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.fromMetadata(metadata) != maxColumnId.get();

    // We are comparing the old schema with the new schema to determine if the schema has changed.
    // If this becomes hotspot, we can consider updating the methods to pass around AtomicBoolean
    // to track if the schema has changed. It is a bit convoluted to pass around and update the
    // AtomicBoolean in the recursive and multiple methods.
    if (oldSchema.equals(newSchema) && !shouldUpdateMaxId) {
      return Optional.empty();
    }

    String maxFieldId = Integer.toString(maxColumnId.get());
    return Optional.of(
        metadata
            .withNewSchema(newSchema)
            .withMergedConfiguration(singletonMap(COLUMN_MAPPING_MAX_COLUMN_ID_KEY, maxFieldId)));
  }

  /**
   * Recursively visits each nested struct / array / map type and assigns an id using the current
   * maximum id as the basis and increments from there. Additionally, assigns a physical name based
   * on a random UUID or re-uses the old display name if the mapping mode is updated on an existing
   * table. Note that key / value fields of a map and the element field of an array are not assigned
   * an id / physical name. Such functionality is actually being handled by {@link
   * ColumnMapping#rewriteFieldIdsForIceberg(StructType, AtomicInteger)}.
   *
   * @param field The current {@link StructField}
   * @param maxColumnId Holds the current maximum id. Value is incremented whenever the current max
   *     id value is used to keep the current value always the max id
   * @param isNewTable Whether this is a new or an existing table. For existing tables the physical
   *     name will be re-used from the old display name
   * @param useColumnIdForPhysicalName Whether we should assign physical names to 'col-[colId]'.
   *     When false uses the default behavior described above.
   * @return A new {@link StructField} with updated metadata under the {@link
   *     ColumnMapping#COLUMN_MAPPING_ID_KEY} and the {@link
   *     ColumnMapping#COLUMN_MAPPING_PHYSICAL_NAME_KEY} keys
   */
  private static StructField transformAndAssignColumnIdAndPhysicalName(
      StructField field,
      AtomicInteger maxColumnId,
      boolean isNewTable,
      boolean useColumnIdForPhysicalName) {
    DataType dataType = field.getDataType();
    if (dataType instanceof StructType) {
      StructType type = (StructType) dataType;
      StructType schema = new StructType();
      for (StructField f : type.fields()) {
        schema =
            schema.add(
                transformAndAssignColumnIdAndPhysicalName(
                    assignColumnIdAndPhysicalNameToField(
                        f, maxColumnId, isNewTable, useColumnIdForPhysicalName),
                    maxColumnId,
                    isNewTable,
                    useColumnIdForPhysicalName));
      }
      return new StructField(field.getName(), schema, field.isNullable(), field.getMetadata());
    } else if (dataType instanceof ArrayType) {
      ArrayType type = (ArrayType) dataType;
      StructField elementField =
          transformAndAssignColumnIdAndPhysicalName(
              type.getElementField(), maxColumnId, isNewTable, useColumnIdForPhysicalName);
      return new StructField(
          field.getName(), new ArrayType(elementField), field.isNullable(), field.getMetadata());
    } else if (dataType instanceof MapType) {
      MapType type = (MapType) dataType;
      StructField key =
          transformAndAssignColumnIdAndPhysicalName(
              type.getKeyField(), maxColumnId, isNewTable, useColumnIdForPhysicalName);
      StructField value =
          transformAndAssignColumnIdAndPhysicalName(
              type.getValueField(), maxColumnId, isNewTable, useColumnIdForPhysicalName);
      return new StructField(
          field.getName(), new MapType(key, value), field.isNullable(), field.getMetadata());
    }
    return field;
  }

  /**
   * Assigns an id using the current maximum id as the basis and increments from there.
   * Additionally, assigns a physical name based on a random UUID or re-uses the old display name if
   * the mapping mode is updated on an existing table.
   *
   * @param field The current {@link StructField} to assign an id / physical name to
   * @param maxColumnId Holds the current maximum id. Value is incremented whenever the current max
   *     id value is used to keep the current value always the max id
   * @param isNewTable Whether this is a new or an existing table. For existing tables the physical
   *     name will be re-used from the old display name
   * @param useColumnIdForPhysicalName Whether we should assign physical names to 'col-[colId]'.
   *     When false uses the default behavior described above.
   * @return A new {@link StructField} with updated metadata under the {@link
   *     ColumnMapping#COLUMN_MAPPING_ID_KEY} and the {@link
   *     ColumnMapping#COLUMN_MAPPING_PHYSICAL_NAME_KEY} keys
   */
  private static StructField assignColumnIdAndPhysicalNameToField(
      StructField field,
      AtomicInteger maxColumnId,
      boolean isNewTable,
      boolean useColumnIdForPhysicalName) {
    if (hasColumnId(field) ^ hasPhysicalName(field)) {
      // If a connector is providing column mapping metadata in the given schema we require it to be
      // complete
      throw new IllegalArgumentException(
          String.format(
              "Both columnId and physicalName must be present if one is present. "
                  + "Found this field with incomplete column mapping metadata: %s",
              field));
    }
    if (!hasColumnId(field)) {
      field =
          field.withNewMetadata(
              FieldMetadata.builder()
                  .fromMetadata(field.getMetadata())
                  .putLong(COLUMN_MAPPING_ID_KEY, maxColumnId.incrementAndGet())
                  .build());
    }
    if (!hasPhysicalName(field)) {
      // re-use old display names as physical names when a table is updated
      String physicalName;
      if (useColumnIdForPhysicalName) {
        long columnId = getColumnId(field);
        physicalName = String.format("col-%s", columnId);
      } else {
        physicalName = isNewTable ? "col-" + UUID.randomUUID() : field.getName();
      }

      field =
          field.withNewMetadata(
              FieldMetadata.builder()
                  .fromMetadata(field.getMetadata())
                  .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, physicalName)
                  .build());
    }
    return field;
  }

  private static boolean hasNestedColumnIds(StructField field) {
    return field.getMetadata().contains(COLUMN_MAPPING_NESTED_IDS_KEY);
  }

  private static FieldMetadata getNestedColumnIds(StructField field) {
    return field.getMetadata().getMetadata(COLUMN_MAPPING_NESTED_IDS_KEY);
  }

  private static int getMaxNestedColumnId(StructField field) {
    return getNestedColumnIds(field).getEntries().values().stream()
        .filter(Long.class::isInstance)
        .map(Long.class::cast)
        .max(Comparator.naturalOrder())
        .orElse(0L)
        .intValue();
  }

  /**
   * Adds the nested field IDs required by Iceberg.
   *
   * <p>In parquet, list-type columns have a nested, implicitly defined {@code element} field and
   * map-type columns have implicitly defined {@code key} and {@code value} fields. By default,
   * Spark does not write field IDs for these fields in the parquet files. However, Iceberg requires
   * these *nested* field IDs to be present. This method rewrites the specified schema to add those
   * nested field IDs.
   *
   * <p>Nested field IDs are stored in a map as part of the metadata of the *nearest* parent {@link
   * StructField}. For example, consider the following schema:
   *
   * <p>col1 ARRAY(INT) col2 MAP(INT, INT) col3 STRUCT(a INT, b ARRAY(STRUCT(c INT, d MAP(INT,
   * INT))))
   *
   * <p>col1 is a list and so requires one nested field ID for the {@code element} field in parquet.
   * This nested field ID will be stored in a map that is part of col1's {@link
   * StructField#getMetadata()}. The same applies to the nested field IDs for col2's implicit {@code
   * key} and {@code value} fields. col3 itself is a Struct, consisting of an integer field and a
   * list field named 'b'. The nested field ID for the list of 'b' is stored in b's {@link
   * StructField#getMetadata()}. Finally, the list type itself is again a struct consisting of an
   * integer field and a map field named 'd'. The nested field IDs for the map of 'd' are stored in
   * d's {@link StructField#getMetadata()}.
   *
   * @param schema The schema to which nested field IDs should be added
   * @param startId The first field ID to use for the nested field IDs
   */
  private static StructType rewriteFieldIdsForIceberg(StructType schema, AtomicInteger startId) {
    StructType newSchema = new StructType();
    for (StructField field : schema.fields()) {
      FieldMetadata.Builder builder = FieldMetadata.builder().fromMetadata(field.getMetadata());
      newSchema =
          newSchema.add(
              transformSchema(
                      startId,
                      field,
                      "",
                      /** current column path */
                      builder)
                  .withNewMetadata(builder.build()));
    }
    return newSchema;
  }

  /**
   * Recursively visits each nested struct / array / map type and returns a new {@link StructField}
   * with updated {@link FieldMetadata}. For array / map types the field IDs of their nested
   * elements are under the {@link ColumnMapping#COLUMN_MAPPING_NESTED_IDS_KEY} key. A concrete
   * schema example can be seen at {@link ColumnMapping#rewriteFieldIdsForIceberg(StructType,
   * AtomicInteger)}.
   *
   * @param currentFieldId The current maximum field id to increment and use for assignment
   * @param structField The field where to start from
   * @param path The current field path relative to the parent field (aka most recent ancestor with
   *     a StructField). An empty path indicates that there's no parent and we're at the root
   * @param closestStructFieldParentMetadata The metadata builder of the closest struct field parent
   *     where nested IDs will be stored. For StructFields this is the current field. For
   *     map/arrays, it is the closest parent that is a struct field.
   * @return A new {@link StructField} with updated {@link FieldMetadata}
   */
  private static StructField transformSchema(
      AtomicInteger currentFieldId,
      StructField structField,
      String path,
      FieldMetadata.Builder closestStructFieldParentMetadata) {
    DataType dataType = structField.getDataType();
    if (dataType instanceof StructType) {
      StructType type = (StructType) dataType;
      List<StructField> fields =
          type.fields().stream()
              .map(
                  field -> {
                    FieldMetadata.Builder metadataBuilder =
                        FieldMetadata.builder().fromMetadata(field.getMetadata());
                    return transformSchema(
                            currentFieldId, field, getPhysicalName(field), metadataBuilder)
                        .withNewMetadata(metadataBuilder.build());
                  })
              .collect(Collectors.toList());
      return new StructField(
          structField.getName(),
          new StructType(fields),
          structField.isNullable(),
          structField.getMetadata());
    } else if (dataType instanceof ArrayType) {
      ArrayType type = (ArrayType) dataType;
      String basePath = "".equals(path) ? getPhysicalName(structField) : path;
      // update element type metadata and recurse into element type
      String elementPath = basePath + "." + type.getElementField().getName();
      maybeUpdateFieldId(closestStructFieldParentMetadata, elementPath, currentFieldId);
      StructField elementType =
          transformSchema(
              currentFieldId,
              type.getElementField(),
              elementPath,
              closestStructFieldParentMetadata);
      return new StructField(
          structField.getName(),
          new ArrayType(elementType),
          structField.isNullable(),
          structField.getMetadata());
    } else if (dataType instanceof MapType) {
      MapType type = (MapType) dataType;
      // update key type metadata and recurse into key type
      String basePath = "".equals(path) ? getPhysicalName(structField) : path;
      String keyPath = basePath + "." + type.getKeyField().getName();
      maybeUpdateFieldId(closestStructFieldParentMetadata, keyPath, currentFieldId);
      StructField key =
          transformSchema(
              currentFieldId, type.getKeyField(), keyPath, closestStructFieldParentMetadata);
      // update value type metadata and recurse into value type
      String valuePath = basePath + "." + type.getValueField().getName();
      maybeUpdateFieldId(closestStructFieldParentMetadata, valuePath, currentFieldId);
      StructField value =
          transformSchema(
              currentFieldId, type.getValueField(), valuePath, closestStructFieldParentMetadata);
      return new StructField(
          structField.getName(),
          new MapType(key, value),
          structField.isNullable(),
          structField.getMetadata());
    }

    return structField;
  }

  /**
   * The {@code field} being passed here is either {@link ArrayType#getElementField()} or one of
   * {@link MapType#getKeyField()}, {@link MapType#getValueField()}. For a map the passed in {@code
   * key} will be one of columnName.key or columnName.value. For an array the passed {@code key}
   * will be columnName.element. The columnName in this case is either the physical or the display
   * name of the column.
   *
   * <p>Below is an example that shows the {@link FieldMetadata} of an array named <b>b</b>, where
   * the array itself is assigned id = 2 with a physical name that includes a UUID. That metadata
   * field then holds a nested {@link FieldMetadata} under the {@code COLUMN_MAPPING_NESTED_IDS_KEY}
   * key as can be seen below, which in turn contains the assigned id.
   *
   * <blockquote>
   *
   * <pre>
   * {
   *   "name": "b",
   *   "type": {
   *     "type": "array",
   *     "elementType": "integer",
   *     "containsNull": true
   *   },
   *   "nullable": true,
   *   "metadata": {
   *     "delta.columnMapping.id": 2,
   *     "delta.columnMapping.physicalName": "col-859d81a5-6e36-4e43-9c8e-46aa7d80dce6"
   *     "delta.columnMapping.nested.ids": {
   *       "col-859d81a5-6e36-4e43-9c8e-46aa7d80dce6.element": 4
   *     },
   *   }
   * }
   * </pre>
   *
   * </blockquote>
   *
   * @param fieldMetadataBuilder The FieldMetadata.Builder to update with nested IDs
   * @param key For a map this is <colName>.key or <colName>.value. For an array this is
   *     <colName>.element
   * @param currentFieldId The current maximum field id to increment and use for assignment
   */
  private static void maybeUpdateFieldId(
      FieldMetadata.Builder fieldMetadataBuilder, String key, AtomicInteger currentFieldId) {
    // init the nested metadata that holds the nested ids
    FieldMetadata nestedMetadata = fieldMetadataBuilder.getMetadata(COLUMN_MAPPING_NESTED_IDS_KEY);
    if (fieldMetadataBuilder.getMetadata(COLUMN_MAPPING_NESTED_IDS_KEY) == null) {
      fieldMetadataBuilder.putFieldMetadata(COLUMN_MAPPING_NESTED_IDS_KEY, FieldMetadata.empty());
      nestedMetadata = fieldMetadataBuilder.getMetadata(COLUMN_MAPPING_NESTED_IDS_KEY);
    }

    // assign an id to the nested element and update the metadata
    if (!nestedMetadata.contains(key)) {
      FieldMetadata newNestedMeta =
          FieldMetadata.builder()
              .fromMetadata(nestedMetadata)
              .putLong(key, currentFieldId.incrementAndGet())
              .build();
      fieldMetadataBuilder.putFieldMetadata(COLUMN_MAPPING_NESTED_IDS_KEY, newNestedMeta);
    }
  }
}
