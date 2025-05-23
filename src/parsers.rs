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

#[derive(Clone, Debug, PartialEq, IntoPyObject)]
pub enum Data {
    F64(f64),
    ISize(isize),
    Timestamp(Timestamp),
    String(String),
}

pub fn parse_timeseries_generic(
    xml_text: &str,
    labels: Vec<&str>,
    metadata: Vec<&str>,
    period_name: &str,
) -> Result<HashMap<String, Vec<Data>>, anyhow::Error> {
    let mut data: HashMap<String, Vec<Data>> = HashMap::new();
    let parser = EventReader::from_str(xml_text);

    let mut current_period_start: Option<String> = None;
    let mut current_period_resolution: Option<String> = None;
    let mut current_position: Option<i64> = None;
    let mut current_label_values: HashMap<String, Data> = HashMap::new();
    let mut current_metadata_values: HashMap<String, Data> = HashMap::new();
    let mut current_element: Option<String> = None;

    for e in parser {
        match e {
            Ok(XmlEvent::StartElement { name, .. }) => {
                current_element = Some(name.local_name.clone());
                if name.local_name == "TimeSeries" {
                    current_metadata_values = HashMap::new();
                }
                if name.local_name == period_name {
                    current_period_start = None;
                    current_period_resolution = None;
                } else if name.local_name == "Point" {
                    current_position = None;
                    current_label_values = HashMap::new();
                }
            }
            Ok(XmlEvent::Characters(text)) => {
                if current_element == Some("start".to_string()) {
                    current_period_start = Some(text);
                } else if current_element == Some("resolution".to_string()) {
                    current_period_resolution = Some(text);
                } else if current_element == Some("position".to_string()) {
                    current_position = Some(text.parse()?);
                } else {
                    for label in &labels {
                        if current_element == Some(label.to_string()) {
                            if let Ok(current_label_value) = text.parse::<isize>() {
                                current_label_values
                                    .entry(label.to_string())
                                    .insert_entry(Data::ISize(current_label_value));
                            } else if let Ok(current_label_value) = text.parse::<f64>() {
                                current_label_values
                                    .entry(label.to_string())
                                    .insert_entry(Data::F64(current_label_value));
                            } else {
                                current_label_values
                                    .entry(label.to_string())
                                    .insert_entry(Data::String(text.to_string()));
                            }
                        }
                    }
                    for metadata_element in &metadata {
                        if current_element == Some(metadata_element.to_string()) {
                            current_metadata_values
                                .entry(metadata_element.to_string())
                                .insert_entry(Data::String(text.to_string()));
                        }
                    }
                }
            }
            Ok(XmlEvent::EndElement { name }) => {
                if let (Some(start), Some(resolution), Some(position)) =
                    (&current_period_start, &current_period_resolution, &current_position)
                {
                    if name.local_name == "Point" {
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
                        data.entry("timestamp".to_string())
                            .or_default()
                            .push(Data::Timestamp(timestamp));
                        for (k, v) in current_label_values.iter() {
                            data.entry(k.to_string()).or_default().push(v.clone());
                        }
                        for (k, v) in current_metadata_values.iter() {
                            data.entry(k.to_string()).or_default().push(v.clone());
                        }
                        data.entry("resolution".to_string())
                            .or_default()
                            .push(Data::String(resolution.clone()));
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
    fn test_parse_timeseries_generic_day_ahead_prices() {
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

        let result = parse_timeseries_generic(xml_text, vec!["price.amount"], vec![], "period");
        assert!(result.is_ok(), "{}", format!("Error: {:?}", result.err().unwrap()));

        let data = result.unwrap();
        assert!(data.contains_key("timestamp"), "{}", format!("Keys: {:?}", data.keys()));
        assert!(
            data.contains_key("price.amount"),
            "{}",
            format!("Keys: {:?}", data.keys())
        );
        assert!(
            data.contains_key("resolution"),
            "{}",
            format!("Keys: {:?}", data.keys())
        );
        assert_eq!(
            data["timestamp"],
            vec![
                Data::Timestamp("2023-12-31T23:00:00Z".parse().unwrap()),
                Data::Timestamp("2024-01-01T00:00:00Z".parse().unwrap()),
            ]
        );
        assert_eq!(data["price.amount"], vec![Data::F64(104.98), Data::F64(105.98)]);
        assert_eq!(
            data["resolution"],
            vec![Data::String("PT60M".to_string()), Data::String("PT60M".to_string())]
        );
    }

    #[test]
    fn test_parse_timeseries_balancy_energy_prices() {
        let xml_text = r#"<?xml version="1.0" encoding="utf-8"?>
        <Balancing_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:balancingdocument:4:1">
        <mRID>05e317126b4f46f3bb4af1f0314ca0e7</mRID>
        <revisionNumber>1</revisionNumber>
        <type>A84</type>
        <process.processType>A16</process.processType>
        <sender_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</sender_MarketParticipant.mRID>
        <sender_MarketParticipant.marketRole.type>A32</sender_MarketParticipant.marketRole.type>
        <receiver_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</receiver_MarketParticipant.mRID>
        <receiver_MarketParticipant.marketRole.type>A33</receiver_MarketParticipant.marketRole.type>
        <createdDateTime>2024-09-24T15:45:51Z</createdDateTime>
        <area_Domain.mRID codingScheme="A01">10YBE----------2</area_Domain.mRID>
        <period.timeInterval>
            <start>2023-09-03T22:00Z</start>
            <end>2023-09-04T22:00Z</end>
        </period.timeInterval>
        <TimeSeries>
            <mRID>1</mRID>
            <businessType>A96</businessType>
            <original_MarketProduct.marketProductType>A02</original_MarketProduct.marketProductType>
            <mktPSRType.psrType>A03</mktPSRType.psrType>
            <flowDirection.direction>A01</flowDirection.direction>
            <currency_Unit.name>EUR</currency_Unit.name>
            <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
            <curveType>A03</curveType>
            <Period>
                <timeInterval>
                    <start>2023-09-03T22:00Z</start>
                    <end>2023-09-04T22:00Z</end>
                </timeInterval>
                <resolution>PT15M</resolution>
                <Point>
                    <position>1</position>
                    <activation_Price.amount>116.17</activation_Price.amount>
                    <imbalance_Price.category>A06</imbalance_Price.category>
                </Point>
                <Point>
                    <position>3</position>
                    <activation_Price.amount>111.17</activation_Price.amount>
                    <imbalance_Price.category>A06</imbalance_Price.category>
                </Point>
            </Period>
        </TimeSeries>
        </Balancing_MarketDocument>
        "#;

        let result = parse_timeseries_generic(
            xml_text,
            vec!["activation_Price.amount"],
            vec!["flowDirection.direction", "businessType"],
            "period",
        );
        assert!(result.is_ok(), "{}", format!("Error: {:?}", result.err().unwrap()));

        let data = result.unwrap();
        assert!(data.contains_key("timestamp"), "{}", format!("Keys: {:?}", data.keys()));
        assert!(
            data.contains_key("activation_Price.amount"),
            "{}",
            format!("Keys: {:?}", data.keys())
        );
        assert!(
            data.contains_key("flowDirection.direction"),
            "{}",
            format!("Keys: {:?}", data.keys())
        );
        assert!(
            data.contains_key("businessType"),
            "{}",
            format!("Keys: {:?}", data.keys())
        );
        assert!(
            data.contains_key("resolution"),
            "{}",
            format!("Keys: {:?}", data.keys())
        );
        assert_eq!(
            data["timestamp"],
            vec![
                Data::Timestamp("2023-09-03T22:00:00Z".parse().unwrap()),
                Data::Timestamp("2023-09-03T22:30:00Z".parse().unwrap()),
            ]
        );
        assert_eq!(
            data["activation_Price.amount"],
            vec![Data::F64(116.17), Data::F64(111.17)]
        );
        assert_eq!(
            data["resolution"],
            vec![Data::String("PT15M".to_string()), Data::String("PT15M".to_string())]
        );
    }
}
