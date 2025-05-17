use chrono::{DateTime, Duration};
use std::collections::HashMap;
use std::str;
use xml::reader::{EventReader, XmlEvent};

fn resolution_to_timedelta(res_text: &str) -> Option<Duration> {
    let resolutions: HashMap<&str, Duration> = [
        ("PT60M", Duration::minutes(60)),
        // ("P1Y", Duration::days(365)), // not exact
        ("PT15M", Duration::minutes(15)),
        ("PT30M", Duration::minutes(30)),
        ("P1D", Duration::days(1)),
        ("P7D", Duration::days(7)),
        // ("P1M", Duration::days(30)), // not exact
        ("PT1M", Duration::minutes(1)),
    ]
    .iter()
    .cloned()
    .collect();
    resolutions.get(res_text).cloned()
}

pub fn parse_timeseries_generic(
    xml_text: &str,
    label: &str,
    period_name: &str,
) -> Result<HashMap<String, Vec<String>>, anyhow::Error> {
    let mut data: HashMap<String, Vec<String>> = HashMap::new();
    let parser = EventReader::from_str(xml_text);

    let mut current_period_start: Option<String> = None;
    let mut current_period_resolution: Option<String> = None;
    let mut current_position: Option<usize> = None;
    let mut current_value: Option<String> = None;
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
                    current_value = Some(text);
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
                        let start = DateTime::parse_from_rfc3339(&start_iso)
                            .map_err(|e| anyhow::anyhow!("Failed to parse start '{}': {}", start, e))?;
                        let delta = resolution_to_timedelta(resolution).unwrap();
                        let timestamp = start + delta * (*position as i32 - 1);
                        data.entry(resolution.clone() + "_timestamp")
                            .or_default()
                            .push(timestamp.to_rfc3339());
                        data.entry(resolution.clone() + "_value")
                            .or_default()
                            .push(value.clone());
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
    use super::parse_timeseries_generic;

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
        assert_eq!(data["PT60M_value"], vec!["104.98", "105.98"]);
    }
}
