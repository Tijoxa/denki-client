use anyhow::anyhow;
use jiff::{Span, Timestamp, ToSpan};
use pyo3::IntoPyObject;
use std::{collections::HashMap, sync::LazyLock};
use xml::reader::{EventReader, XmlEvent};

static RESOLUTIONS: LazyLock<HashMap<&'static str, Span>> = LazyLock::new(|| {
    HashMap::from([
        ("PT60M", 60.minutes()),
        ("P1Y", 1.year()),
        ("PT15M", 15.minutes()),
        ("PT30M", 30.minutes()),
        ("P1D", 1.day()),
        ("P7D", 7.days()),
        ("P1M", 1.month()),
        ("PT1M", 1.minute()),
    ])
});

#[derive(Debug, PartialEq, IntoPyObject)]
pub enum Data {
    F64(f64),
    Timestamp(Timestamp),
}

pub fn parse_timeseries_generic(
    xml_text: &str,
    label: &str,
    period_name: &str,
) -> Result<HashMap<String, Vec<Data>>, anyhow::Error> {
    let mut data: HashMap<String, Vec<Data>> = HashMap::new();
    let parser = EventReader::from_str(xml_text);

    let mut current_period_start: Option<String> = None;
    let mut current_period_resolution: Option<String> = None;
    let mut current_position: Option<i64> = None;
    let mut current_value: Option<f64> = None;
    let mut current_element: Option<String> = None;

    for e in parser {
        match e {
            Ok(XmlEvent::StartElement { name, .. }) => {
                current_element = Some(name.local_name.clone());
                if name.local_name == period_name {
                    current_period_start = None;
                    current_period_resolution = None;
                } else if name.local_name == "Point" {
                    current_position = None;
                    current_value = None;
                }
            }
            Ok(XmlEvent::Characters(text)) => {
                if current_element == Some("start".to_string()) {
                    current_period_start = Some(text);
                } else if current_element == Some("resolution".to_string()) {
                    current_period_resolution = Some(text);
                } else if current_element == Some("position".to_string()) {
                    current_position = Some(text.parse()?);
                } else if current_element == Some(label.to_string()) {
                    current_value = Some(text.parse::<f64>()?);
                }
            }
            Ok(XmlEvent::EndElement { name }) => {
                if name.local_name == "Point" {
                    if let (Some(start), Some(resolution), Some(position), Some(value)) = (
                        &current_period_start,
                        &current_period_resolution,
                        &current_position,
                        &current_value,
                    ) {
                        let start_iso = if start.ends_with("Z") {
                            start.replace("Z", ":00Z")
                        } else {
                            start.clone() + ":00"
                        };
                        let start: Timestamp = start_iso.parse()?;
                        let delta = RESOLUTIONS
                            .get(resolution.as_str())
                            .ok_or(anyhow!("Resolution not found"))?;
                        let timestamp = start + *delta * (position - 1);
                        data.entry(resolution.clone() + "_timestamp")
                            .or_default()
                            .push(Data::Timestamp(timestamp.clone()));
                        data.entry(resolution.clone() + "_value")
                            .or_default()
                            .push(Data::F64(value.clone()));
                    }
                }
            }
            Err(e) => return Err(e.into()),
            _ => {}
        }
    }

    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::{parse_timeseries_generic, Data};

    #[test]
    fn test_parse_timeseries_generic() {
        let xml_text = r#"<?xml version="1.0" encoding="utf-8"?>
        <publication_marketdocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">
        <mRID>bf4445f7e6e04c849b7e0830b906fbde</mRID>
        <revisionnumber>1</revisionnumber>
        <type>A44</type>
        <sender_marketparticipant.mRID codingscheme="A01">10X1001A1001A450</sender_marketparticipant.mRID>
        <sender_marketparticipant.marketrole.type>A32</sender_marketparticipant.marketrole.type>
        <receiver_marketparticipant.mRID codingscheme="A01">10X1001A1001A450</receiver_marketparticipant.mRID>
        <receiver_marketparticipant.marketrole.type>A33</receiver_marketparticipant.marketrole.type>
        <createddatetime>2025-05-17T21:13:31Z</createddatetime>
        <period.timeInterval>
            <start>2023-12-31T23:00Z</start>
            <end>2024-01-01T23:00Z</end>
        </period.timeInterval>
        <TimeSeries>
            <mRID>1</mRID>
            <auction.type>A01</auction.type>
            <businessType>A62</businessType>
            <in_Domain.mRID codingscheme="A01">10YFR-RTE------C</in_Domain.mRID>
            <out_Domain.mRID codingscheme="A01">10YFR-RTE------C</out_Domain.mRID>
            <contract_MarketAgreement.type>A01</contract_MarketAgreement.type>
            <currency_Unit.name>EUR</currency_Unit.name>
            <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
            <curveType>A03</curveType>
            <Period>
                <timeInterval>
                    <start>2023-12-31T23:00Z</start>
                    <end>2024-01-01T23:00Z</end>
                </timeInterval>
                <resolution>PT60M</resolution>
                <Point>
                    <position>1</position>
                    <price.amount>104.98</price.amount>
                </Point>
                <Point>
                    <position>2</position>
                    <price.amount>105.98</price.amount>
                </Point>
            </Period>
        </TimeSeries>
        </publication_marketdocument>
        "#;

        let result = parse_timeseries_generic(xml_text, "price.amount", "period");
        assert!(result.is_ok(), "{}", format!("Error: {:?}", result.err().unwrap()));

        let data = result.unwrap();
        assert!(
            data.contains_key("PT60M_timestamp"),
            "{}",
            format!("Keys: {:?}", data.keys())
        );
        assert!(
            data.contains_key("PT60M_value"),
            "{}",
            format!("Keys: {:?}", data.keys())
        );
        assert_eq!(
            data["PT60M_timestamp"],
            vec![
                Data::Timestamp("2023-12-31T23:00:00Z".parse().unwrap()),
                Data::Timestamp("2024-01-01T00:00:00Z".parse().unwrap()),
            ]
        );
        assert_eq!(data["PT60M_value"], vec![Data::F64(104.98), Data::F64(105.98)]);
    }
}
