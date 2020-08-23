use reqwest::StatusCode;
use csv::ReaderBuilder;
use serde::{Deserialize, Serialize, de::Deserializer, ser::Serializer};

use std::fmt::{Display, Formatter};
use std::path::Path;
use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct StressTestParams<'a> {
    pub queries_total: u32,
    pub initial_documents: usize,
    // pub threads_number: u16,
    pub data_path: &'a Path,
    pub test_existing_index: Option<String>,
    pub no_wait_after_updates: bool,
    pub no_update_queries: bool,
}

pub struct StressTestResult {
    pub all_queries_send_time_ms: u128,
    pub all_queries_receive_time_ms: u128,
    pub all_updates_commited_time_ms: u128,
    pub avg_success_response_time: u128,
    pub successful_responses: u32,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CsvDocument {
    title: String,
    price_str: String,
    thumbnail_path: String,
    #[serde(deserialize_with = "deserialize_lossy")]
    about: String,
    url: String,
}


#[derive(Serialize, Clone)]
pub struct Document<'a> {
    pub id: String,
    #[serde(flatten)]
    pub doc: &'a CsvDocument,
    pub dummy: i32,
}

pub struct TypesenseDocuments<'a, I: Iterator<Item=Document<'a>> + Clone>(pub I);
impl<'a, I: Iterator<Item=Document<'a>> + Clone> Serialize for TypesenseDocuments<'a, I> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        self.0.clone()
            .map(|doc| serde_json::json!(doc))
            .join("\n")
            .serialize::<_>(serializer)
    }
    
} 

impl<'a, I: Iterator<Item=Document<'a>> + Clone> Display for TypesenseDocuments<'a, I> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.serialize(f)
    }
}

fn deserialize_lossy<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let s: &[u8] = serde::Deserialize::deserialize(deserializer)?;
    Ok(String::from_utf8_lossy(s).to_owned().to_string())
}

pub struct SerializeIterator<T: Serialize, I: Iterator<Item=T>> (
    pub I
);

impl<T, I> Serialize for SerializeIterator<T, I> where I: Iterator<Item=T> + Clone, T: Serialize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        serializer.collect_seq(self.0.clone())
    }
}



pub fn load_test_data(path: &Path) -> Result<Vec<CsvDocument>, csv::Error> {
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .trim(csv::Trim::All)
        .from_path(path)?;
    let data: Vec<_> = reader.byte_records()
        .map(|result| {
            match result {
                Ok(record) => record.deserialize::<CsvDocument>(None).ok(),
                Err(e) => { 
                    println!("{}", e.to_string());
                    None
                }
            }
        })
        .filter_map(|e| e)
        .collect();
    // data.iter_mut()
    //     .enumerate()
    //     .for_each(|(idx, &mut e)| e.id = idx + 1);
    Ok(data)
}

#[derive(Debug)]
pub struct BadHttpStatusError {
    pub expected_code: StatusCode,
    pub received_code: StatusCode,
    pub response_body: serde_json::Value,
    pub method: &'static str,
}

impl Display for BadHttpStatusError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Query type {} responded with unexpected HTTP code: \"{}\" expected, \"{}\" received", 
            self.method, self.expected_code, self.received_code) // user-facing output
    }
}

impl std::error::Error for BadHttpStatusError {}