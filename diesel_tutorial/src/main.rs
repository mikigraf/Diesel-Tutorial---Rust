#![recursion_limit = "128"]

#[macro_use]
extern crate diesel;
#[macro_use]
extern crate diesel_infer_schema;

extern crate dotenv;

use diesel::prelude::*;
use diesel::mysql::MysqlConnection;
use dotenv::dotenv;
use std::env;

pub mod schema {
    infer_schema!("dotenv:DB_URL");
}

use schema::*;

#[derive(Queryable, Insertable)]
#[table_name = "tag"]
pub struct Tag {
    pub tag_id: i16,
    pub tag_name: String,
}

pub fn establish_connection() -> MysqlConnection {
    dotenv().ok();
    let db_url: String = String::from(env::var("DB_URL").expect("DB_URL must be set"));
    let db_connection =
        MysqlConnection::establish(&db_url).expect(&format!("Error connecting to {}", &db_url));

    return db_connection;
}

fn read_and_output(db_connection: &MysqlConnection) {

    let results = tag::table.load::<Tag>(db_connection).expect("problem");

    println!("Returned results: {}", results.len());

    for r in results {
        println!("{} {}", r.tag_id, r.tag_name);
    }
}

pub fn insert_tag(db_connection: &MysqlConnection, tag_id_val: i16, tag_name_val: String) {

    let new_tag = Tag {
        tag_id: tag_id_val,
        tag_name: tag_name_val,
    };

    diesel::insert_into(tag::table)
        .values(&new_tag)
        .execute(db_connection)
        .expect("Error inserting");
}

fn main() {
    let db_connection = establish_connection();

    read_and_output(&db_connection);

    // create a new tag and insert it
    let tag_id: i16 = 23;
    let tag_name: String = String::from("basketball");

    insert_tag(&db_connection, tag_id, tag_name);

    // let's check if our insert has been succesful
    read_and_output(&db_connection);

}
