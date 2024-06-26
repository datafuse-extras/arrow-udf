#[derive(StructType)]
struct Data {
    null: (),
    boolean: bool,
    int8: i8,
    int16: i16,
    int32: i32,
    int64: i64,
    uint8: u8,
    uint16: u16,
    uint32: u32,
    uint64: u64,
    float32: f32,
    float64: f64,
    decimal: Decimal,
    date: NaiveDate,
    time: NaiveTime,
    timestamp: NaiveDateTime,
    interval: Interval,
    json: serde_json::Value,
    string: String,
    binary: Vec<u8>,
    string_array: Vec<String>,
    struct_: KeyValue<'static>,
}
