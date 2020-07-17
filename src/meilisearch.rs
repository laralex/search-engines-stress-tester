use serde::Deserialize;
use reqwest::{ StatusCode, Error, Url };

use crate::actions::TestDocument;
#[derive(Deserialize)]
struct Index
{
    uid: String,
}

pub enum Query<'a> {
    ListIndexes,
    GetIndex(&'a str),

    GetDocument(&'a str, &'a str), // index + doc id
    GetDocumentsBatch(&'a str, usize, usize), // index + docs offset + docs limit
    AddOrReplaceDocuments(&'a str, &'a [TestDocument]), // index + doc
    AddOrUpdateDocuments(&'a str, &'a [TestDocument]), // index + doc
    DeleteDocument(&'a str, &'a str), // index + doc id
    DeleteDocumentsBatch(&'a str, &'a [&'a str]), // index + docs ids

    SearchDocument(&'a str, &'a str, usize), // index + query + limit
}

pub enum StressTestQuery<'a> {
    ListIndexes,
    GetRndIndex(&'a [&'a str]),

    //GetRndDocument(&'a)
}

pub fn purge(url: Url) -> Result<(), Error> {
    let indexes_resourse = url.join("indexes/").unwrap();
    let indexes : Vec<Index> = reqwest::blocking::get(indexes_resourse.as_str())?.json()?;
    let client = reqwest::blocking::Client::new();
    indexes.iter().for_each(|index| {
        let index_resourse = indexes_resourse.join(&index.uid).unwrap();
        match client.delete(index_resourse.as_str()).send() {
            Ok(response) => {
                match response.status() {
                    StatusCode::NO_CONTENT => println!("Deleted index: {}", index_resourse),
                    StatusCode::NOT_FOUND => println!("Cannot find index: {}", index_resourse),
                    status_code => println!("Unknown response {:?} from index: {}", status_code, index_resourse)
                };
            },
            Err(e) => {
                println!("Failed to send request to index {}", index_resourse);
            },
        };
    });
    Ok( () )
}

pub fn add_index(url: &Url, name: &str, primary_key: &str) {

}

pub fn delete_index(url: &Url, name: &str) {

}

pub fn list_indexes(url: &Url) {

}

pub fn get_index(url: &Url, name: &str) {

}

pub fn search_document(url: &Url, index: &str, query: String) {

}

pub fn add_document(url: &Url, index: &str) {

}

//} // pub mod meilisearch