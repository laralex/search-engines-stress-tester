use csv::ReaderBuilder;
use serde::{Deserialize, Serialize, de::Deserializer};

use std::path::Path;

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
    pub id: usize,
    #[serde(flatten)]
    pub doc: &'a CsvDocument,
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