use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use quick_xml::events::Event;
use quick_xml::Reader;
use std::collections::HashMap;
use std::str;

#[derive(Debug)]
pub enum ParseError {
    UnrecognizedTimedelta,
}

pub fn resolution_to_timedelta(res_text: &str) -> Result<Duration, ParseError> {
    let resolutions: HashMap<&str, Duration> = [
        ("PT60M", Duration::minutes(60)),
        ("P1Y", Duration::days(365)), // not exact
        ("PT15M", Duration::minutes(15)),
        ("PT30M", Duration::minutes(30)),
        ("P1D", Duration::days(1)),
        ("P7D", Duration::days(7)),
        ("P1M", Duration::days(30)), // not exact
        ("PT1M", Duration::minutes(1)),
    ]
    .iter()
    .cloned()
    .collect();
    resolutions
        .get(res_text)
        .cloned()
        .ok_or(ParseError::UnrecognizedTimedelta)
}

pub fn parse_timeseries_generic(xml: &str, label: &str) -> Result<HashMap<String, Vec<String>>, ParseError> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);
    let mut buf = Vec::new();
    let mut data = HashMap::new();
    let mut in_period = false;
    let mut start = String::new();
    let mut delta_text = String::new();
    let mut delta = Duration::zero();

    loop {
        match reader.read_event(&mut buf) {
            Ok(Event::Start(ref e)) if e.name() == b"period" => {
                in_period = true;
            }
            Ok(Event::End(ref e)) if e.name() == b"period" => {
                in_period = false;
            }
            Ok(Event::Start(ref e)) if in_period && e.name() == b"start" => {
                if let Ok(Event::Text(e)) = reader.read_event(&mut buf) {
                    start = e.unescape_and_decode(&reader).unwrap();
                }
            }
            Ok(Event::Start(ref e)) if in_period && e.name() == b"resolution" => {
                if let Ok(Event::Text(e)) = reader.read_event(&mut buf) {
                    delta_text = e.unescape_and_decode(&reader).unwrap();
                    delta = resolution_to_timedelta(&delta_text)?;
                }
            }
            Ok(Event::Start(ref e)) if in_period && e.name() == b"point" => {
                let mut value_text = String::new();
                let mut position = 0;
                loop {
                    match reader.read_event(&mut buf) {
                        Ok(Event::Start(ref e)) if e.name() == b"quantity" => {
                            if let Ok(Event::Text(e)) = reader.read_event(&mut buf) {
                                value_text = e.unescape_and_decode(&reader).unwrap();
                            }
                        }
                        Ok(Event::Start(ref e)) if e.name() == b"position" => {
                            if let Ok(Event::Text(e)) = reader.read_event(&mut buf) {
                                position = e.unescape_and_decode(&reader).unwrap().parse::<i64>().unwrap();
                            }
                        }
                        Ok(Event::End(ref e)) if e.name() == b"point" => {
                            let timestamp = parse_datetime(&start, "UTC")? + (position - 1) * delta;
                            data.entry(format!("{}_timestamp", delta_text))
                                .or_insert(Vec::new())
                                .push(timestamp.to_rfc3339());
                            data.entry(format!("{}_value", delta_text))
                                .or_insert(Vec::new())
                                .push(value_text);
                            break;
                        }
                        _ => (),
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
            _ => (),
        }
        buf.clear();
    }
    Ok(data)
}

pub fn parse_timeseries_generic_whole(xml: &str, label: &str) -> Result<Vec<HashMap<String, Vec<String>>>, ParseError> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);
    let mut buf = Vec::new();
    let mut data_all = Vec::new();
    let mut in_timeseries = false;
    let mut timeseries_xml = String::new();

    loop {
        match reader.read_event(&mut buf) {
            Ok(Event::Start(ref e)) if e.name() == b"timeseries" => {
                in_timeseries = true;
                timeseries_xml.clear();
                timeseries_xml.push_str("<timeseries>");
            }
            Ok(Event::End(ref e)) if e.name() == b"timeseries" => {
                in_timeseries = false;
                timeseries_xml.push_str("</timeseries>");
                let data = parse_timeseries_generic(&timeseries_xml, label)?;
                data_all.push(data);
            }
            Ok(Event::Eof) => break,
            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
            _ => (),
        }
        if in_timeseries {
            timeseries_xml.push_str(&String::from_utf8_lossy(&buf));
        }
        buf.clear();
    }
    Ok(data_all)
}
