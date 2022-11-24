/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

use std::collections::HashMap;

use crate::graph_client::connection::Connection;
use crate::graph_client::connection_pool::ConnectionPool_nebula;
use crate::graph_client::nebula_schema::Tag;
use crate::graph_client::nebula_schema::ColType;
use crate::graph_client::nebula_schema::InsertTagQuery;
use crate::graph_client::nebula_schema::InsertEdgeQueryWithRank;

pub struct Session<'a> {
    session_id: i64,
    conn: Connection,
    pool: &'a ConnectionPool_nebula,
    username: String,
    password: String,
    // empty means not a named timezone
    time_zone_name: String,
    // Offset to utc in seconds
    offset_secs: i32,
    // Keep connection if true
    retry_connect: bool,
}

impl<'a> Session<'a> {
    pub fn new(
        session_id: i64,
        conn: Connection,
        pool: &'a ConnectionPool_nebula,
        username: String,
        password: String,
        time_zone_name: String,
        offset_secs: i32,
        retry_connect: bool,
    ) -> Self {
        Session {
            session_id: session_id,
            conn: conn,
            pool: pool,
            username: username,
            password: password,
            time_zone_name: time_zone_name,
            offset_secs: offset_secs,
            retry_connect: retry_connect,
        }
    }

    /// sign out the session
    #[inline]
    pub async fn signout(&self) -> std::result::Result<(), common::types::ErrorCode> {
        self.conn.signout(self.session_id).await
    }

    /// Execute the query in current session
    /// The returned error of `Result` only means the request/response status
    /// The error from Nebula Graph is still in `error_code` field in response, so you need check it
    /// to known wether the query execute succeeded
    #[inline]
    pub async fn execute(
        &self,
        query: &str,
    ) -> std::result::Result<graph::types::ExecutionResponse, common::types::ErrorCode> {
        self.conn.execute(self.session_id, query).await
    }

    /// Get the time zone name
    #[inline]
    pub fn time_zone_name(&self) -> &str {
        &self.time_zone_name
    }

    /// Get the time zone offset to UTC in seconds
    #[inline]
    pub fn offset_secs(&self) -> i32 {
        self.offset_secs
    }
    #[inline]
    pub async fn show_spaces(&self){
        let query = "show spaces;";
        let resp = self.execute(query).await.unwrap();
        resp.show_data();
    }

    #[inline]
    pub fn get_create_space_query(&self, space_name: &str, partition_num: u8, replica_factor: u8, is_fixed_string: bool, fixed_string_len: u8, comment: &str) -> String{
        let mut query = String::from("CREATE SPACE IF NOT EXISTS `");
        query += space_name;
        query += "` (partition_num = ";
        query += partition_num.to_string().as_str();
        query += ", replica_factor = ";
        query += replica_factor.to_string().as_str();
        query += ", vid_type = ";
        if is_fixed_string{
            query += "FIXED_STRING(";
            query += fixed_string_len.to_string().as_str();
            query += "))";
        }else{
            query += "INT64)";
        }
        if comment!=""{
            query += " COMMENT = \"";
            query += comment;
            query += "\"";
        }
        query += ";";
        query
    }
    #[inline]
    // CREATE SPACE `testGraph` (partition_num = 15, replica_factor = 1, vid_type = FIXED_STRING(50)) COMMENT = "this is a graph for test"
    pub async fn create_space(&self, space_name: &str, partition_num: u8, replica_factor: u8, is_fixed_string: bool, fixed_string_len: u8, comment: &str){
        let query = self.get_create_space_query(space_name, partition_num, replica_factor, is_fixed_string, fixed_string_len, comment);
        // println!("{}", query);
        let _resp = self.execute(query.as_str()).await.unwrap();
        // println!("{:?}", _resp);
    }

    #[inline]
    pub fn get_create_tag_or_edge(&self, space_name: &str, col_type: ColType, tag_name: &str, comment: &str, tags: Vec<Tag>) -> String{
        let mut query = String::from("use ");
        query += space_name;
        query += "; CREATE ";
        query += col_type.to_string().as_str();
        query += " IF NOT EXISTS `";
        query += tag_name;
        query += "` (";
        for i in 0..tags.len() {
            query += tags[i].to_string().as_str();
            if i!=tags.len()-1{
                query += ",";
            }
        }
        query += ")";
        if comment!="".to_string(){
            query += " COMMENT = \"";
            query += comment;
            query += "\"";
        }
        query += ";";
        query
    }

    #[inline]
    pub async fn create_tag_or_edge(&self, space_name: &str, col_type: ColType, tag_name: &str, comment: &str, tags: Vec<Tag>){

        let query = self.get_create_tag_or_edge(space_name, col_type, tag_name, comment, tags);
        //println!("{}", query);

        let _resp = self.execute(query.as_str()).await.unwrap();

        //println!("{:?}", _resp);
    }

    #[inline]
    // INSERT VERTEX t2 (name, age) VALUES "11":("n1", 12);
    pub async fn insert_tag(&self, space_name: &str, tag_name: &str, kv: HashMap<String, String>, vid: &str){

        if self.find_tag_or_edge(space_name, tag_name, ColType::Tag).await == false{
            std::thread::sleep(std::time::Duration::from_millis(5000));
        }

        let mut query = String::from("use ");
        query += space_name;
        query += "; ";
        query += "INSERT VERTEX IF NOT EXISTS ";
        query += tag_name;
        query += " ";
        let mut keys = String::from("(");
        let mut values = String::from("(");
        for (k,v) in kv{
            if keys.len()!=1{
                keys += ",";
                values += ",";
            }
            keys += k.as_str();
            values += v.as_str();
        }
        keys += ")";
        values += ")";
        query += keys.as_str();
        query += " VALUES \"";
        query += vid;
        query += "\":";
        query += values.as_str();
        query += ";";

        // println!("{}", query);
        let _resp = self.execute(query.as_str()).await.unwrap();
        // println!("{:?}", _resp);
    }

    #[inline]
    pub async fn insert_tags(&self, insert_tag_queries: Vec<InsertTagQuery>){
        for query in insert_tag_queries{
            self.insert_tag(query.space_name.as_str(), query.tag_name.as_str(), query.kv, query.vid.as_str()).await;
        }
    }

    #[inline]
    pub async fn insert_edges(&self, insert_edge_queries: Vec<InsertEdgeQueryWithRank>){
        for query in insert_edge_queries{
            self.insert_edge_with_rank(query.space_name.as_str(), query.edge_name.as_str(), query.kv, query.from_vertex.as_str(), query.to_vertex.as_str(), query.rank).await;
        }
    }

    #[inline]
    // INSERT EDGE e2 (name, age) VALUES "11"->"13":("n1", 12);
    pub async fn insert_edge(&self, space_name: &str, edge_name: &str, kv: HashMap<String, String>, from_vertex: &str, to_vertex: &str){

        if self.find_tag_or_edge(space_name, edge_name, ColType::Edge).await == false{
            std::thread::sleep(std::time::Duration::from_millis(5000));
        }

        let mut query = String::from("use ");
        query += space_name;
        query += "; ";
        query += "INSERT EDGE IF NOT EXISTS ";
        query += edge_name;
        query += " ";
        let mut keys = String::from("(");
        let mut values = String::from("(");
        for (k,v) in kv{
            if keys.len()!=1{
                keys += ",";
                values += ",";
            }
            keys += k.as_str();
            values += v.as_str();
        }
        keys += ")";
        values += ")";
        query += keys.as_str();
        query += " VALUES \"";
        query += from_vertex;
        query += "\" -> \"";
        query += to_vertex;
        query += "\":";
        query += values.as_str();
        query += ";";

        // println!("{}", query);
        let _resp = self.execute(query.as_str()).await.unwrap();
    }


    #[inline]
    // INSERT EDGE e2 (name, age) VALUES "11"->"13"@1:("n1", 12);
    pub async fn insert_edge_with_rank(&self, space_name: &str, edge_name: &str, kv: HashMap<String, String>, from_vertex: &str, to_vertex: &str, rank: i64){
        let mut query = String::from("use ");
        query += space_name;
        query += "; ";
        query += "INSERT EDGE IF NOT EXISTS ";
        query += edge_name;
        query += " ";
        let mut keys = String::from("(");
        let mut values = String::from("(");
        for (k,v) in kv{
            if keys.len()!=1{
                keys += ",";
                values += ",";
            }
            keys += k.as_str();
            values += v.as_str();
        }
        keys += ")";
        values += ")";
        query += keys.as_str();
        query += " VALUES \"";
        query += from_vertex;
        query += "\" -> \"";
        query += to_vertex;
        query += "\"@";
        query += rank.to_string().as_str();
        query += ":";
        query += values.as_str();
        query += ";";

        // println!("{}", query);
        let _resp = self.execute(query.as_str()).await.unwrap();
    
        println!("{:?}", _resp);
    }

    #[inline]
    // CREATE TAG INDEX `index_tag` on `stu`      (`name`(10), `age`) COMMENT "this is an index for tag"
    pub async fn create_index(&self, space_name: &str, index_type: ColType, tag_or_edge_name: &str, index_name: &str, comment: &str, indexed_properties: HashMap<String, u8>){
        let mut query = String::from("use ");
        query += space_name;
        query += "; ";
        query += "CREATE ";
        query += index_type.to_string().as_str();
        query += " INDEX `";
        query += index_name;
        query += "` on `";
        query += tag_or_edge_name;
        query += "`(";

        let mut properties = String::from("");
        for (k,v) in indexed_properties {
            if properties.len()!=0{
                properties += ",";
            }
            let mut property = String::from("`");
            property += k.as_str();
            property += "`";
            if v==0 {
            }else{
                property += "(";
                property += v.to_string().as_str();
                property += ")"
            }
            properties += property.as_str();
        }

        query += properties.as_str();
        query += ") ";
        if comment!=""{
            query += "COMMENT \"";
            query += comment;
            query += "\"";
        }
        query += ";";

        // println!("{}", query);
        let _resp = self.execute(query.as_str()).await.unwrap();
        // println!("{:?}", _resp);
    }

    #[inline]
    pub async fn find_tag_or_edge(&self, space_name: &str, tag_or_edge_name: &str, col_type: ColType) -> bool{
        let mut query = Self::use_space(space_name);
        match col_type {
            ColType::Edge => query += "show edges;",
            ColType::Tag => query += "show tags;",
        }
        let resp = self.execute(query.as_str()).await.unwrap();
        // print!("{:?}", resp);
        let res = resp.get_sVal();
        match res {
            Some(tags) => {
                for tag in tags{
                    if tag == tag_or_edge_name.to_string(){
                        return true;
                    }
                }
            }
            None => return false
        }
        return false;
    }

    #[inline]
    pub fn use_space(space_name: &str) -> String{
        let mut line = String::from("use ");
        line += space_name;
        line += ";";
        line
    }

}

impl<'a> Drop for Session<'a> {
    /// Drop session will sign out the session in server
    /// and give back connection to pool
    fn drop(&mut self) {
        futures::executor::block_on(self.signout());
        self.pool.give_back(std::mem::take(&mut self.conn));
    }
}
