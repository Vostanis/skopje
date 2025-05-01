/// Convert a u32 timestamp to a chrono::NaiveDate.
pub fn convert_timestamp(timestamp: u32) -> chrono::NaiveDate {
    chrono::DateTime::from_timestamp(timestamp.into(), 0)
        .expect("failed to convert timestamp integer")
        .date_naive()
}

/// Convert a &String to a chrono::NaiveDate (so that it can inserted directly as DATE)
pub fn convert_date_type(str_date: &String) -> anyhow::Result<chrono::NaiveDate> {
    let date = chrono::NaiveDate::parse_from_str(&str_date, "%Y-%m-%d").map_err(|err| {
        tracing::error!("failed to parse date string; expected form YYYYMMDD - received: {str_date}, error({err})");
        err
    })?;
    Ok(date)
}
