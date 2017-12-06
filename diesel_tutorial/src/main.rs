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

#[derive(Queryable, Insertable, Identifiable)]
#[table_name = "tag"]
#[primary_key(tag_id)]
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

fn update_tag(db_connection : &MysqlConnection, id : i16, new_value : String){
    diesel::update(tag::table.find(id))
        .set(tag::tag_name.eq(new_value))
        .execute(db_connection);
}

pub fn delete_tag(db_connection : &MysqlConnection, tag_id_val: i16){
    diesel::delete(tag::table.find(tag_id_val))
        .execute(db_connection);

}

fn main() {
    let db_connection = establish_connection();

    // 1. query data from the table
    read_and_output(&db_connection);

    // 2. insert new data into the table
    let tag_id: i16 = 777;
    let tag_name: String = String::from("educational");
    insert_tag(&db_connection, tag_id, tag_name);
    read_and_output(&db_connection);

    // 3. update existing data
    update_tag(&db_connection, tag_id, String::from("science"));
    read_and_output(&db_connection);

    // 4. delete data from the table
    delete_tag(&db_connection, tag_id);
    read_and_output(&db_connection);

}
