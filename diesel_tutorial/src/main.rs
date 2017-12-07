#![recursion_limit = "128"]

#[macro_use]
extern crate diesel;
#[macro_use]
extern crate diesel_infer_schema;

extern crate dotenv;

use diesel::prelude::*;
use diesel::mysql::MysqlConnection;
use diesel::sql_query;
use diesel::result::QueryResult;
use std::vec::Vec;
use diesel::expression::sql_literal::sql;
use diesel::dsl::count;
use diesel::expression::count::Count;

use dotenv::dotenv;
use std::env;

pub mod schema {
    infer_schema!("dotenv:DB_URL");
}

use schema::*;

#[derive(Queryable, Insertable, Identifiable, QueryableByName)]
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

pub fn select_n_1(db_connection: &MysqlConnection, n: i8){
    let n: String = n.to_string();
    let query = format!("SELECT * FROM tag.tag LIMIT {},1",n);
    let users: QueryResult<Vec<Tag>>  = sql_query(query).load(db_connection);
    match users {
    Ok(v) => {
        for tag in &v {
            println!("working with version: {:?}", tag.tag_name)
        }
    },
    Err(e) => println!("error parsing header: {:?}", e),
    }
}

fn main() {
    let db_connection = establish_connection();
    
    // 1. query data from the table
    read_and_output(&db_connection);

    // 2. insert new data into the table
    let tag_id: i16 = 345;
    let tag_name: String = String::from("vlog");
    insert_tag(&db_connection, tag_id, tag_name);
    read_and_output(&db_connection);

    // 3. update existing data
    update_tag(&db_connection, tag_id, String::from("travel"));
    read_and_output(&db_connection);

    // 4. delete data from the table
    delete_tag(&db_connection, tag_id);
    read_and_output(&db_connection);

    // 5. Custom SQL, counting and batchwise
    let table_size: i8 = sql("select count(*) from tag.tag")
    .get_result(&db_connection)
    .expect("Error executing raw SQL");

    let mut x = 0;
    while x < table_size {
        select_n_1(&db_connection, x);
        println!("End of iteration");
        x = x + 1;
    }

}
