use serde::Deserialize;
use reqwest::{ StatusCode, Error, Url };

#[derive(Deserialize)]
struct TypesenseCollections
{
    collections: Option<Vec<TypesenseCollection>>,
}

#[derive(Deserialize)]
struct TypesenseCollection 
{
    name: String,
}

pub fn purge(url: Url, api_key: String) -> Result<(), Error> {
    let all_collections_resourse = url.join("collections/").unwrap();
    let client = reqwest::blocking::Client::new();
    //let all_collections : TypesenseCollections 
    let all_collections_response = client.get(all_collections_resourse.as_str())
        .header("X-TYPESENSE-API-KEY", api_key)
        .send()?;
    //println!("{:?}", all_collections_response.text()?);
    if all_collections_response.status() != StatusCode::OK {
        println!("Failed to get all collections: {:?}", all_collections_response.text()?);
        return Ok( () );
    }
    let all_collections : TypesenseCollections = all_collections_response.json()?;
    if let Some(collections) = all_collections.collections {
        collections.iter().for_each(|collection| {
            println!("Deleted collection: {}", collection.name);
        });
    }
    //println!(all_collections.text()?);
        //.json()?;
    // all_collections.collections.iter().for_each(|collection| {
    //     let collection_resourse = all_collections_resourse.join(&collection.name).unwrap();
    //     let delete_result = client.delete(collection_resourse.as_str())
    //         .header("X-TYPESENSE-API-KEY", "ABCD")
    //         .send();
    //     match delete_result {
    //         Ok(response) => {
    //             match response.status() {
    //                 StatusCode::NO_CONTENT => println!("Deleted collection: {}", collection_resourse),
    //                 StatusCode::NOT_FOUND => println!("Cannot find collection: {}", collection_resourse),
    //                 status_code => println!("Unknown response {:?} from collection: {}", status_code, collection_resourse)
    //             };
    //         },
    //         Err(e) => {
    //             println!("Failed to send request to collection {}", collection_resourse);
    //         },
    //     };
    // });
    Ok( () )
}