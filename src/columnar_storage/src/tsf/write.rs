// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Arrow `RecordBatch` -> TSF v1 bytes.
//!
//! The flush and compaction paths produce sorted batches in the storage
//! schema; this module converts them into the flat `Row` model of
//! `storage_kernel::tsf` and encodes a single immutable file (ADR-0002 §3).
//! `storage_kernel` stays Arrow-free, so the conversion lives here.

use arrow::{
    array::{Array, Float64Array, Int64Array, RecordBatch, UInt64Array},
    datatypes::DataType,
};
use storage_kernel::tsf::{
    file::{encode_file, FileStats, WriterOptions},
    Row,
};

use crate::types::{StorageSchema, RESERVED_COLUMN_NAME, SEQ_COLUMN_NAME};

/// Converts a sorted batch into rows for the TSF writer.
///
/// Phase 2 restricts TSF to the sample schema `(metric_id, tsid, timestamp,
/// value)`: three `u64`/`i64` primary keys and one `f64` value. Other schemas
/// are rejected so a misconfigured `format = "tsf"` fails loudly instead of
/// silently dropping columns.
pub fn batch_to_rows(batch: &RecordBatch, schema: &StorageSchema) -> Result<Vec<Row>, String> {
    if schema.num_primary_keys != 3 {
        return Err(format!(
            "TSF v1 requires exactly 3 primary keys (metric_id, tsid, timestamp); got {}",
            schema.num_primary_keys
        ));
    }
    if schema.value_idxes.len() != 1 {
        return Err(format!(
            "TSF v1 supports exactly one value column; got {}",
            schema.value_idxes.len()
        ));
    }

    let metric_id = uint64_column(batch, 0, "metric_id")?;
    let series_id = uint64_column(batch, 1, "tsid")?;
    let timestamps = int64_column(batch, 2, "timestamp")?;
    let values = float64_column(batch, schema.value_idxes[0], "value")?;
    let sequences = uint64_column(batch, schema.seq_idx, SEQ_COLUMN_NAME)?;
    let reserved = nullable_uint64_column(batch, schema.reserved_idx, RESERVED_COLUMN_NAME)?;

    let rows = batch.num_rows();
    let mut out = Vec::with_capacity(rows);
    for i in 0..rows {
        out.push(Row {
            metric_id: metric_id.value(i),
            series_id: series_id.value(i),
            timestamp: timestamps.value(i),
            value: values.value(i),
            sequence: sequences.value(i),
            // `__reserved__` is nullable in the storage schema and the write
            // path leaves it unset; a null encodes as 0.
            reserved: if reserved.is_null(i) {
                0
            } else {
                reserved.value(i)
            },
        });
    }
    Ok(out)
}

fn uint64_column<'a>(
    batch: &'a RecordBatch,
    idx: usize,
    name: &str,
) -> Result<&'a UInt64Array, String> {
    let column = batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            format!(
                "TSF column `{name}` must be UInt64, got {:?}",
                batch.schema().field(idx).data_type()
            )
        })?;
    if column.null_count() != 0 {
        return Err(format!("TSF column `{name}` must not contain nulls"));
    }
    Ok(column)
}

/// Downcasts a nullable `UInt64` column. Nulls are allowed; callers decide how
/// to interpret them.
fn nullable_uint64_column<'a>(
    batch: &'a RecordBatch,
    idx: usize,
    name: &str,
) -> Result<&'a UInt64Array, String> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            format!(
                "TSF column `{name}` must be UInt64, got {:?}",
                batch.schema().field(idx).data_type()
            )
        })
}

fn int64_column<'a>(
    batch: &'a RecordBatch,
    idx: usize,
    name: &str,
) -> Result<&'a Int64Array, String> {
    let column = batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            format!(
                "TSF column `{name}` must be Int64, got {:?}",
                batch.schema().field(idx).data_type()
            )
        })?;
    if column.null_count() != 0 {
        return Err(format!("TSF column `{name}` must not contain nulls"));
    }
    Ok(column)
}

fn float64_column<'a>(
    batch: &'a RecordBatch,
    idx: usize,
    name: &str,
) -> Result<&'a Float64Array, String> {
    if batch.schema().field(idx).data_type() != &DataType::Float64 {
        return Err(format!(
            "TSF column `{name}` must be Float64, got {:?}",
            batch.schema().field(idx).data_type()
        ));
    }
    let column = batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| format!("cannot downcast TSF column `{name}` to Float64"))?;
    if column.null_count() != 0 {
        return Err(format!("TSF column `{name}` must not contain nulls"));
    }
    Ok(column)
}

/// Encodes the rows of one sorted batch into TSF v1 bytes.
///
/// `partition` is the inclusive/exclusive day bounds of the file and must
/// contain every timestamp in the batch.
pub fn encode_batch(
    batch: &RecordBatch,
    schema: &StorageSchema,
    partition: (i64, i64),
) -> Result<(Vec<u8>, FileStats), String> {
    let rows = batch_to_rows(batch, schema)?;
    encode_file(rows, partition, &WriterOptions::default())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use storage_kernel::tsf::file::TsfReader;

    use super::*;
    use crate::config::UpdateMode;

    fn sample_schema() -> StorageSchema {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("metric_id", DataType::UInt64, false),
            Field::new("tsid", DataType::UInt64, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        StorageSchema::try_new(arrow_schema, 3, UpdateMode::Overwrite).unwrap()
    }

    #[test]
    fn test_encode_batch_round_trips() {
        use arrow::array::{Float64Array, Int64Array, UInt64Array};

        let schema = sample_schema();
        let batch = RecordBatch::try_new(
            schema.arrow_schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![1, 1])),
                Arc::new(UInt64Array::from(vec![10, 10])),
                Arc::new(Int64Array::from(vec![5, 7])),
                Arc::new(Float64Array::from(vec![1.5, 2.5])),
                Arc::new(UInt64Array::from(vec![100, 101])),
                Arc::new(UInt64Array::from(vec![0, 0])),
            ],
        )
        .unwrap();

        let (bytes, stats) = encode_batch(&batch, &schema, (0, 10)).unwrap();
        assert_eq!(stats.rows, 2);

        let reader = TsfReader::from_bytes(bytes).unwrap();
        assert_eq!(reader.metric_ids(), vec![1]);
        let rows = reader.read_all().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].series_id, 10);
        assert_eq!(rows[0].timestamp, 5);
        assert_eq!(rows[1].sequence, 101);
    }

    #[test]
    fn test_encode_batch_rejects_unsupported_schema() {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let schema = StorageSchema::try_new(arrow_schema, 1, UpdateMode::Overwrite).unwrap();
        assert!(schema.arrow_schema.fields().len() >= 2);
        // A zero-row batch cannot be encoded; the schema check fires first.
        let batch = RecordBatch::new_empty(schema.arrow_schema.clone());
        assert!(batch_to_rows(&batch, &schema).is_err());
    }
}
