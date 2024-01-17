// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use arrow_array::{
    types::*, BinaryArray, Int32Array, LargeBinaryArray, LargeStringArray, ListArray, RecordBatch,
    StringArray,
};
use arrow_cast::pretty::pretty_format_batches;
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_js::{CallMode, Runtime};

#[test]
fn test_gcd() {
    let mut runtime = Runtime::new().unwrap();

    let js_code = r#"
        export function gcd(a, b) {
            while (b != 0) {
                let t = b;
                b = a % b;
                a = t;
            }
            return a;
        }
    "#;
    runtime
        .add_function(
            "gcd",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            js_code,
        )
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Int32, true),
    ]);
    let arg0 = Int32Array::from(vec![Some(25), None]);
    let arg1 = Int32Array::from(vec![Some(15), None]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("gcd", &input).unwrap();
    assert_eq!(
        pretty_format_batches(std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+-----+
| gcd |
+-----+
| 5   |
|     |
+-----+
"#
        .trim()
    );
}

#[test]
fn test_to_string() {
    let mut runtime = Runtime::new().unwrap();

    let js_code = r#"
        export function to_string(a) {
            if (a == null) {
                return "null";
            }
            return a.toString();
        }
    "#;
    runtime
        .add_function(
            "to_string",
            DataType::Utf8,
            CallMode::CalledOnNullInput,
            js_code,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(5), None]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("to_string", &input).unwrap();
    assert_eq!(
        pretty_format_batches(std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+-----------+
| to_string |
+-----------+
| 5         |
| null      |
+-----------+
"#
        .trim()
    );
}

#[test]
fn test_concat() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "concat",
            DataType::Binary,
            CallMode::ReturnNullOnNullInput,
            r#"
            export function concat(a, b) {
                return a.concat(b);
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("a", DataType::Binary, true),
        Field::new("b", DataType::Binary, true),
    ]);
    let arg0 = BinaryArray::from(vec![&b"hello"[..]]);
    let arg1 = BinaryArray::from(vec![&b"world"[..]]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("concat", &input).unwrap();
    assert_eq!(
        pretty_format_batches(std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+----------------------+
| concat               |
+----------------------+
| 68656c6c6f776f726c64 |
+----------------------+
"#
        .trim()
    );
}

#[test]
fn test_json_array_access() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "json_array_access",
            DataType::LargeUtf8,
            CallMode::ReturnNullOnNullInput,
            r#"
            export function json_array_access(array, i) {
                return array[i];
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("array", DataType::LargeUtf8, true),
        Field::new("i", DataType::Int32, true),
    ]);
    let arg0 = LargeStringArray::from(vec![r#"[1, null, ""]"#]);
    let arg1 = Int32Array::from(vec![0]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("json_array_access", &input).unwrap();
    assert_eq!(
        pretty_format_batches(std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+-------------------+
| json_array_access |
+-------------------+
| 1                 |
+-------------------+
"#
        .trim()
    );
}

#[test]
fn test_json_stringify() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "json_stringify",
            DataType::Utf8,
            CallMode::ReturnNullOnNullInput,
            r#"
            export function json_stringify(object) {
                return JSON.stringify(object);
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("json", DataType::LargeUtf8, true)]);
    let arg0 = LargeStringArray::from(vec![r#"[1, null, ""]"#]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("json_stringify", &input).unwrap();
    assert_eq!(
        pretty_format_batches(std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+----------------+
| json_stringify |
+----------------+
| [1,null,""]    |
+----------------+
"#
        .trim()
    );
}

#[test]
fn test_decimal_add() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "decimal_add",
            DataType::LargeBinary,
            CallMode::ReturnNullOnNullInput,
            r#"
            export function decimal_add(a, b) {
                return a + b;
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("a", DataType::LargeBinary, true),
        Field::new("b", DataType::LargeBinary, true),
    ]);
    let arg0 = LargeBinaryArray::from(vec![b"0.0001".as_ref()]);
    let arg1 = LargeBinaryArray::from(vec![b"0.0002".as_ref()]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("decimal_add", &input).unwrap();
    assert_eq!(
        pretty_format_batches(std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+--------------+
| decimal_add  |
+--------------+
| 302e30303033 |
+--------------+
"#
        .trim()
    );
}

#[test]
fn test_typed_array() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "object_type",
            DataType::Utf8,
            CallMode::ReturnNullOnNullInput,
            r#"
            export function object_type(a) {
                return Object.prototype.toString.call(a);
            }
            "#,
        )
        .unwrap();

    /// Generate a record batch with a single column of type `List<T>`.
    fn array_input<T: ArrowPrimitiveType>() -> RecordBatch {
        let schema = Schema::new(vec![Field::new(
            "x",
            DataType::new_list(T::DATA_TYPE, true),
            true,
        )]);
        let arg0 =
            ListArray::from_iter_primitive::<T, _, _>(vec![Some(vec![Some(Default::default())])]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap()
    }

    let cases = [
        // (input, JS object type)
        (array_input::<Int8Type>(), "Int8Array"),
        (array_input::<Int16Type>(), "Int16Array"),
        (array_input::<Int32Type>(), "Int32Array"),
        (array_input::<Int64Type>(), "BigInt64Array"),
        (array_input::<UInt8Type>(), "Uint8Array"),
        (array_input::<UInt16Type>(), "Uint16Array"),
        (array_input::<UInt32Type>(), "Uint32Array"),
        (array_input::<UInt64Type>(), "BigUint64Array"),
        (array_input::<Float32Type>(), "Float32Array"),
        (array_input::<Float64Type>(), "Float64Array"),
    ];

    for (input, expected) in cases.iter() {
        let output = runtime.call("object_type", input).unwrap();
        let object_type = output
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(object_type, format!("[object {}]", expected));
    }
}

#[test]
fn test_return_array() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "to_array",
            DataType::new_list(DataType::Int32, true),
            CallMode::CalledOnNullInput,
            r#"
            export function to_array(x) {
                if(x == null) {
                    return null;
                }
                return [x];
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("to_array", &input).unwrap();
    assert_eq!(
        pretty_format_batches(std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+----------+
| to_array |
+----------+
| [1]      |
|          |
| [3]      |
+----------+
    "#
        .trim()
    );
}

#[test]
fn test_range() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "range",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
            export function* range(n) {
                for (let i = 0; i < n; i++) {
                    yield i;
                }
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let mut outputs = runtime.call_table_function("range", &input, 2).unwrap();
    let o1 = outputs.next().unwrap().unwrap();
    let o2 = outputs.next().unwrap().unwrap();
    assert_eq!(o1.num_rows(), 2);
    assert_eq!(o2.num_rows(), 2);
    assert!(outputs.next().is_none());

    assert_eq!(
        pretty_format_batches(&[o1, o2]).unwrap().to_string(),
        r#"
+-----+-------+
| row | range |
+-----+-------+
| 0   | 0     |
| 2   | 0     |
| 2   | 1     |
| 2   | 2     |
+-----+-------+
"#
        .trim()
    );
}
