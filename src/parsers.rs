use anyhow::anyhow;
use jiff::{tz::TimeZone, Span, Timestamp, ToSpan};
use pyo3::IntoPyObject;
use std::{collections::HashMap, sync::LazyLock};
use xml::reader::{EventReader, XmlEvent};

#[derive(Clone, Debug, PartialEq)]
pub struct XmlAttribute {
    pub name: String,
    pub namespace: Option<String>,
    pub value: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct XmlNode {
    pub name: String,
    pub namespace: Option<String>,
    pub text: Option<String>,
    pub attributes: Vec<XmlAttribute>,
    pub children: Vec<XmlNode>,
}

/// Parse one XML document while retaining its local names, namespace URIs,
/// attributes, text, child order, and repeated children.
pub fn parse_xml(xml_bytes: &[u8]) -> Result<XmlNode, anyhow::Error> {
    let parser = EventReader::new(xml_bytes);
    let mut stack: Vec<XmlNode> = Vec::new();
    let mut root: Option<XmlNode> = None;

    for event in parser {
        match event? {
            XmlEvent::StartElement { name, attributes, .. } => {
                let node = XmlNode {
                    name: name.local_name,
                    namespace: name.namespace,
                    text: None,
                    attributes: attributes
                        .into_iter()
                        .map(|attribute| XmlAttribute {
                            name: attribute.name.local_name,
                            namespace: attribute.name.namespace,
                            value: attribute.value,
                        })
                        .collect(),
                    children: Vec::new(),
                };
                stack.push(node);
            }
            XmlEvent::Characters(text) | XmlEvent::CData(text) => {
                if let Some(node) = stack.last_mut() {
                    append_text(node, &text);
                }
            }
            XmlEvent::Whitespace(text) => {
                if let Some(node) = stack.last_mut() {
                    append_text(node, &text);
                }
            }
            XmlEvent::EndElement { .. } => {
                let mut node = stack.pop().ok_or_else(|| anyhow!("Unexpected XML end element"))?;
                if node.text.as_deref().is_some_and(|text| text.trim().is_empty()) {
                    node.text = None;
                }
                if let Some(parent) = stack.last_mut() {
                    parent.children.push(node);
                } else if root.replace(node).is_some() {
                    return Err(anyhow!("XML document contains multiple root elements"));
                }
            }
            _ => {}
        }
    }

    if !stack.is_empty() {
        return Err(anyhow!("XML document ended before all elements were closed"));
    }

    root.ok_or_else(|| anyhow!("XML document has no root element"))
}

fn append_text(node: &mut XmlNode, text: &str) {
    match &mut node.text {
        Some(current) => current.push_str(text),
        None => node.text = Some(text.to_string()),
    }
}

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
    Null(Option<String>),
}

pub fn parse_timeseries_generic(
    xml_text: &str,
    labels: Vec<&str>,
    metadata: Vec<&str>,
    period_name: &str,
) -> Result<HashMap<String, Vec<Data>>, anyhow::Error> {
    parse_timeseries_generic_bytes(xml_text.as_bytes(), labels, metadata, period_name)
}

pub fn parse_timeseries_generic_bytes(
    xml_bytes: &[u8],
    labels: Vec<&str>,
    metadata: Vec<&str>,
    period_name: &str,
) -> Result<HashMap<String, Vec<Data>>, anyhow::Error> {
    let document = parse_xml(xml_bytes)?;
    let mut data: HashMap<String, Vec<Data>> = HashMap::new();

    let mut time_series = Vec::new();
    collect_named_descendants(&document, "TimeSeries", &mut time_series);

    let mut projected_columns = Vec::new();
    for name in labels.iter().chain(metadata.iter()) {
        if !projected_columns.contains(name) {
            projected_columns.push(*name);
        }
    }

    for series in time_series {
        let series_metadata = collect_values_outside_periods(series, period_name, &metadata);
        let mut periods = Vec::new();
        collect_descendants_except(series, period_name, "TimeSeries", &mut periods);

        for period in periods {
            let Some(start_text) = period_start(period) else {
                continue;
            };
            let Some(resolution) = direct_child_text(period, "resolution") else {
                continue;
            };
            let Some(delta) = RESOLUTIONS.get(resolution) else {
                let has_point = period.children.iter().any(|child| child.name == "Point");
                if has_point {
                    return Err(anyhow!("Resolution not found: {resolution}"));
                }
                continue;
            };
            let start = parse_period_start(start_text)?;

            for point in period.children.iter().filter(|child| child.name == "Point") {
                let Some(position_text) = direct_child_text(point, "position") else {
                    continue;
                };
                let position = position_text.trim().parse::<i64>()?;
                let timestamp = start
                    .to_zoned(TimeZone::UTC)
                    .checked_add(*delta * (position - 1))?
                    .timestamp();

                let mut row_values = collect_values(point, &labels);
                for (name, value) in &series_metadata {
                    row_values.entry(name.clone()).or_insert_with(|| value.clone());
                }

                data.entry("timestamp".to_string())
                    .or_default()
                    .push(Data::Timestamp(timestamp));
                for name in &projected_columns {
                    data.entry((*name).to_string())
                        .or_default()
                        .push(row_values.remove(*name).unwrap_or(Data::Null(None)));
                }
                data.entry("resolution".to_string())
                    .or_default()
                    .push(Data::String(resolution.to_string()));
            }
        }
    }

    Ok(data)
}

fn collect_named_descendants<'a>(node: &'a XmlNode, name: &str, result: &mut Vec<&'a XmlNode>) {
    for child in &node.children {
        if child.name == name {
            result.push(child);
        }
        collect_named_descendants(child, name, result);
    }
}

fn collect_descendants_except<'a>(
    node: &'a XmlNode,
    target_name: &str,
    excluded_name: &str,
    result: &mut Vec<&'a XmlNode>,
) {
    for child in &node.children {
        if names_match(&child.name, excluded_name) {
            continue;
        }
        if names_match(&child.name, target_name) {
            result.push(child);
        }
        collect_descendants_except(child, target_name, excluded_name, result);
    }
}

fn collect_values_outside_periods(
    node: &XmlNode,
    period_name: &str,
    requested_names: &[&str],
) -> HashMap<String, Data> {
    let mut result = HashMap::new();
    collect_values_excluding(node, period_name, "TimeSeries", requested_names, false, &mut result);
    result
}

fn collect_values_excluding(
    node: &XmlNode,
    excluded_name: &str,
    second_excluded_name: &str,
    requested_names: &[&str],
    parse_numeric: bool,
    result: &mut HashMap<String, Data>,
) {
    for child in &node.children {
        if names_match(&child.name, excluded_name) || child.name == second_excluded_name {
            continue;
        }
        if requested_names.contains(&child.name.as_str()) {
            if let Some(text) = child.text.as_deref() {
                let value = if parse_numeric {
                    parse_label_value(text)
                } else {
                    Data::String(text.trim().to_string())
                };
                result.insert(child.name.clone(), value);
            }
        }
        collect_values_excluding(
            child,
            excluded_name,
            second_excluded_name,
            requested_names,
            parse_numeric,
            result,
        );
    }
}

fn collect_values(node: &XmlNode, requested_names: &[&str]) -> HashMap<String, Data> {
    let mut result = HashMap::new();
    collect_values_excluding(node, "Point", "Point", requested_names, true, &mut result);
    result
}

fn names_match(name: &str, expected: &str) -> bool {
    name == expected || name.eq_ignore_ascii_case(expected)
}

fn direct_child_text<'a>(node: &'a XmlNode, name: &str) -> Option<&'a str> {
    node.children
        .iter()
        .find(|child| child.name == name)
        .and_then(|child| child.text.as_deref())
        .map(str::trim)
}

fn period_start(period: &XmlNode) -> Option<&str> {
    let interval = period.children.iter().find(|child| child.name == "timeInterval")?;
    direct_child_text(interval, "start")
}

fn parse_period_start(start: &str) -> Result<Timestamp, anyhow::Error> {
    let start = start.trim();
    let (date, time) = start
        .split_once('T')
        .ok_or_else(|| anyhow!("Invalid period start: {start}"))?;
    let zone_start = time.find(['Z', '+', '-']).unwrap_or(time.len());
    let (clock, zone) = time.split_at(zone_start);
    let clock = if clock.matches(':').count() == 1 {
        format!("{clock}:00")
    } else {
        clock.to_string()
    };
    let zone = if zone.is_empty() { "Z" } else { zone };
    format!("{date}T{clock}{zone}").parse::<Timestamp>().map_err(Into::into)
}

fn parse_label_value(text: &str) -> Data {
    let text = text.trim();
    if let Ok(value) = text.parse::<isize>() {
        Data::ISize(value)
    } else if let Ok(value) = text.parse::<f64>() {
        Data::F64(value)
    } else {
        Data::String(text.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_timeseries_generic, parse_timeseries_generic_bytes, parse_xml, Data};

    #[test]
    fn test_parse_xml_preserves_namespaces_dotted_names_and_order() {
        let xml = br#"<doc:Root xmlns:doc="urn:document" xmlns="urn:default" xmlns:meta="urn:metadata">
            <TimeSeries>
                <mRID>series-id</mRID>
                <in_Domain.mRID meta:codingScheme="A01">10YBE----------2</in_Domain.mRID>
                <Point><position>1</position></Point>
                <Point><position>2</position></Point>
            </TimeSeries>
        </doc:Root>"#;

        let root = parse_xml(xml).unwrap();
        assert_eq!(root.name, "Root");
        assert_eq!(root.namespace.as_deref(), Some("urn:document"));
        assert_eq!(root.children.len(), 1);

        let series = &root.children[0];
        assert_eq!(series.name, "TimeSeries");
        assert_eq!(series.namespace.as_deref(), Some("urn:default"));
        assert_eq!(series.children[0].text.as_deref(), Some("series-id"));

        let domain = &series.children[1];
        assert_eq!(domain.name, "in_Domain.mRID");
        assert_eq!(domain.namespace.as_deref(), Some("urn:default"));
        assert_eq!(domain.text.as_deref(), Some("10YBE----------2"));
        assert_eq!(domain.attributes.len(), 1);
        assert_eq!(domain.attributes[0].name, "codingScheme");
        assert_eq!(domain.attributes[0].namespace.as_deref(), Some("urn:metadata"));
        assert_eq!(domain.attributes[0].value, "A01");

        let points = &series.children[2..];
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].children[0].text.as_deref(), Some("1"));
        assert_eq!(points[1].children[0].text.as_deref(), Some("2"));
    }

    #[test]
    fn test_parse_xml_accepts_bytes_with_xml_declaration() {
        let root =
            parse_xml(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><document><value>ok</value></document>").unwrap();
        assert_eq!(root.name, "document");
        assert_eq!(root.children[0].text.as_deref(), Some("ok"));
    }

    #[test]
    fn test_parse_xml_rejects_invalid_xml() {
        assert!(parse_xml(b"<document><value></document>").is_err());
    }

    #[test]
    fn test_parse_timeseries_generic_accepts_xml_bytes() {
        let xml = br#"<Document><TimeSeries><Period>
            <timeInterval><start>2024-01-01T00:00Z</start></timeInterval>
            <resolution>PT60M</resolution>
            <Point><position>1</position><quantity>12.5</quantity></Point>
        </Period></TimeSeries></Document>"#;

        let data = parse_timeseries_generic_bytes(xml, vec!["quantity"], vec![], "Period").unwrap();
        assert_eq!(data["quantity"], vec![Data::F64(12.5)]);
        assert_eq!(
            data["timestamp"],
            vec![Data::Timestamp("2024-01-01T00:00:00Z".parse().unwrap())]
        );
    }

    #[test]
    fn test_parse_timeseries_generic_scopes_periods_and_aligns_missing_values() {
        let xml = br#"<doc:Document xmlns:doc="urn:test">
            <doc:period.timeInterval><doc:start>1999-01-01T00:00Z</doc:start></doc:period.timeInterval>
            <doc:TimeSeries>
                <doc:businessType>A96</doc:businessType>
                <doc:Period>
                    <doc:timeInterval><doc:start>2024-01-01T00:00Z</doc:start></doc:timeInterval>
                    <doc:resolution>PT60M</doc:resolution>
                    <doc:Point><doc:position>1</doc:position><doc:quantity>12.5</doc:quantity></doc:Point>
                    <doc:Point><doc:position>2</doc:position></doc:Point>
                </doc:Period>
                <doc:Period>
                    <doc:timeInterval><doc:start>2024-01-01T03:00Z</doc:start></doc:timeInterval>
                    <doc:resolution>PT60M</doc:resolution>
                    <doc:Point><doc:position>1</doc:position><doc:quantity>13</doc:quantity></doc:Point>
                </doc:Period>
            </doc:TimeSeries>
            <doc:TimeSeries>
                <doc:Period>
                    <doc:timeInterval><doc:start>2024-01-01T05:00Z</doc:start></doc:timeInterval>
                    <doc:resolution>PT30M</doc:resolution>
                    <doc:Point>
                        <doc:position>2</doc:position>
                        <doc:quantity>20</doc:quantity>
                        <doc:price.amount>3.0</doc:price.amount>
                    </doc:Point>
                </doc:Period>
            </doc:TimeSeries>
        </doc:Document>"#;

        let data = parse_timeseries_generic(
            std::str::from_utf8(xml).unwrap(),
            vec!["quantity", "price.amount"],
            vec!["businessType"],
            "period",
        )
        .unwrap();

        assert_eq!(
            data["timestamp"],
            vec![
                Data::Timestamp("2024-01-01T00:00:00Z".parse().unwrap()),
                Data::Timestamp("2024-01-01T01:00:00Z".parse().unwrap()),
                Data::Timestamp("2024-01-01T03:00:00Z".parse().unwrap()),
                Data::Timestamp("2024-01-01T05:30:00Z".parse().unwrap()),
            ]
        );
        assert_eq!(
            data["quantity"],
            vec![Data::F64(12.5), Data::Null(None), Data::ISize(13), Data::ISize(20)]
        );
        assert_eq!(
            data["price.amount"],
            vec![Data::Null(None), Data::Null(None), Data::Null(None), Data::F64(3.0)]
        );
        assert_eq!(
            data["businessType"],
            vec![
                Data::String("A96".to_string()),
                Data::String("A96".to_string()),
                Data::String("A96".to_string()),
                Data::Null(None),
            ]
        );
        assert_eq!(
            data["resolution"],
            vec![
                Data::String("PT60M".to_string()),
                Data::String("PT60M".to_string()),
                Data::String("PT60M".to_string()),
                Data::String("PT30M".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_timeseries_generic_day_ahead_price() {
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
    fn test_parse_timeseries_balancy_energy_price() {
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

    #[test]
    fn test_parse_timeseries_installed_capacity_per_production_type() {
        let xml_text = r#"<?xml version="1.0" encoding="UTF-8"?>
        <GL_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0">
        <mRID>7c96d034d7e34768995d82b8602f4800</mRID>
        <revisionNumber>1</revisionNumber>
        <type>A68</type>
        <process.processType>A33</process.processType>
        <sender_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</sender_MarketParticipant.mRID>
        <sender_MarketParticipant.marketRole.type>A32</sender_MarketParticipant.marketRole.type>
        <receiver_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</receiver_MarketParticipant.mRID>
        <receiver_MarketParticipant.marketRole.type>A33</receiver_MarketParticipant.marketRole.type>
        <createdDateTime>2023-08-18T16:15:35Z</createdDateTime>
        <time_Period.timeInterval>
            <start>2022-12-31T23:00Z</start>
            <end>2023-12-31T23:00Z</end>
        </time_Period.timeInterval>
        <TimeSeries>
            <mRID>1</mRID>
            <businessType>A37</businessType>
            <objectAggregation>A08</objectAggregation>
            <inBiddingZone_Domain.mRID codingScheme="A01">10YBE----------2</inBiddingZone_Domain.mRID>
            <quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name>
            <curveType>A01</curveType>
            <MktPSRType>
                <psrType>B01</psrType>
            </MktPSRType>
            <Period>
                <timeInterval>
                    <start>2022-12-31T23:00Z</start>
                    <end>2023-12-31T23:00Z</end>
                </timeInterval>
                <resolution>P1Y</resolution>
                <Point>
                    <position>1</position>
                    <quantity>712</quantity>
                </Point>
            </Period>
        </TimeSeries>

        <TimeSeries>
            <mRID>2</mRID>
            <businessType>A37</businessType>
            <objectAggregation>A08</objectAggregation>
            <inBiddingZone_Domain.mRID codingScheme="A01">10YBE----------2</inBiddingZone_Domain.mRID>
            <quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name>
            <curveType>A01</curveType>
            <MktPSRType>
                <psrType>B04</psrType>
            </MktPSRType>
            <Period>
                <timeInterval>
                    <start>2022-12-31T23:00Z</start>
                    <end>2023-12-31T23:00Z</end>
                </timeInterval>
                <resolution>P1Y</resolution>
                <Point>
                    <position>1</position>
                    <quantity>6915</quantity>
                </Point>
                <Point>
                    <position>2</position>
                    <quantity>6913</quantity>
                </Point>
            </Period>
        </TimeSeries>
        </GL_MarketDocument>
        "#;

        let result = parse_timeseries_generic(
            xml_text,
            vec!["quantity"],
            vec!["quantity_Measure_Unit.name", "psrType"],
            "period",
        );
        assert!(result.is_ok(), "{}", format!("Error: {:?}", result.err().unwrap()));

        let data = result.unwrap();
        assert!(data.contains_key("timestamp"), "{}", format!("Keys: {:?}", data.keys()));
        assert!(data.contains_key("quantity"), "{}", format!("Keys: {:?}", data.keys()));
        assert!(
            data.contains_key("quantity_Measure_Unit.name"),
            "{}",
            format!("Keys: {:?}", data.keys())
        );
        assert!(data.contains_key("psrType"), "{}", format!("Keys: {:?}", data.keys()));
        assert!(
            data.contains_key("resolution"),
            "{}",
            format!("Keys: {:?}", data.keys())
        );
        assert_eq!(
            data["timestamp"],
            vec![
                Data::Timestamp("2022-12-31T23:00:00Z".parse().unwrap()),
                Data::Timestamp("2022-12-31T23:00:00Z".parse().unwrap()),
                Data::Timestamp("2023-12-31T23:00:00Z".parse().unwrap()),
            ]
        );
        assert_eq!(
            data["quantity"],
            vec![Data::ISize(712), Data::ISize(6915), Data::ISize(6913)]
        );
        assert_eq!(
            data["resolution"],
            vec![
                Data::String("P1Y".to_string()),
                Data::String("P1Y".to_string()),
                Data::String("P1Y".to_string())
            ]
        );
    }
}
