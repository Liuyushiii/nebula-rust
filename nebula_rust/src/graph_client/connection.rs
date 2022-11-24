use std::collections::HashMap;
/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
use std::io::Result;

use fbthrift::BinaryProtocol;
use fbthrift_transport::{tokio_io::transport::AsyncTransport, AsyncTransportConfiguration};
use graph::client;
use graph::client::GraphService;
use tokio::net::TcpStream;

use crate::graph_client::transport_response_handler;
use crate::graph_client::pool_config::PoolConfig;
use crate::graph_client::nebula_schema::Tag;
use crate::graph_client::nebula_schema::ColType;
use crate::graph_client::nebula_schema::InsertTagQuery;
use crate::graph_client::nebula_schema::InsertEdgeQueryWithRank;

/// The simple abstraction of a connection to nebula graph server
#[derive(Default)]
pub struct Connection {
    // The option is used to construct a null connection
    // which is used to give back the connection to pool from session
    // So we could assume it's alway not null
    client: Option<
        client::GraphServiceImpl<
            BinaryProtocol,
            AsyncTransport<TcpStream, transport_response_handler::GraphTransportResponseHandler>,
        >,
    >,
}

impl Connection {
    /// Create connection with the specified [host:port] address
    /// 使用指定的 [host:port] 地址创建连接
    pub async fn new_from_address(address: &str) -> Result<Connection> {
        let stream = TcpStream::connect(address).await?;
        let transport = AsyncTransport::new(
            stream,
            AsyncTransportConfiguration::new(
                transport_response_handler::GraphTransportResponseHandler,
            ),
        );
        Ok(Connection {
            client: Some(client::GraphServiceImpl::new(transport)),
        })
    }

    /// Create connection with the specified [host:port]
    pub async fn new(host: &str, port: i32) -> Result<Connection> {
        let address = format!("{}:{}", host, port);
        Connection::new_from_address(&address).await
    }

    /// Create connection from nebula configuration
    pub async fn new_from_conf(conf: &PoolConfig) -> Result<Connection> {
        let address = conf.addresses[0].clone();
        Connection::new_from_address(&address).await
    }

    /// Authenticate by username and password
    /// The returned error of `Result` only means the request/response status
    /// The error from Nebula Graph is still in `error_code` field in response, so you need check it
    /// to known wether authenticate succeeded
    /// 通过用户名和密码进行身份验证 Result 返回的错误仅表示请求/响应状态 Nebula Graph 的错误仍在响应中的 error_code 字段中，
    /// 因此您需要检查它以知道身份验证是否成功
    pub async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> std::result::Result<graph::types::AuthResponse, common::types::ErrorCode> {
        let result = self
            .client
            .as_ref()
            .unwrap()
            .authenticate(
                &username.to_string().into_bytes(),
                &password.to_string().into_bytes(),
            )
            .await;
        if let Err(_) = result {
            return Err(common::types::ErrorCode::E_RPC_FAILURE);
        }
        Ok(result.unwrap())
    }

    /// Sign out the authentication by session id which got by authenticating previous
    /// The returned error of `Result` only means the request/response status
    pub async fn signout(
        &self,
        session_id: i64,
    ) -> std::result::Result<(), common::types::ErrorCode> {
        let result = self.client.as_ref().unwrap().signout(session_id).await;
        if let Err(_) = result {
            return Err(common::types::ErrorCode::E_RPC_FAILURE);
        }
        Ok(())
    }

    /// Execute the query with current session id which got by authenticating previous
    /// The returned error of `Result` only means the request/response status
    /// The error from Nebula Graph is still in `error_code` field in response, so you need check it
    /// to known wether the query execute succeeded
    /// 使用当前会话id执行查询，通过之前的认证得到结果返回的错误只表示请求/响应状态.
    /// NebulaGraph的错误仍然在响应的error_code字段中，所以你需要检查它是否知道查询执行成功
    pub async fn execute(
        &self,
        session_id: i64,
        query: &str,
    ) -> std::result::Result<graph::types::ExecutionResponse, common::types::ErrorCode> {
        let result = self
            .client
            .as_ref()
            .unwrap()
            .execute(session_id, &query.to_string().into_bytes())
            .await;
        if let Err(_) = result {
            return Err(common::types::ErrorCode::E_RPC_FAILURE);
        }
        Ok(result.unwrap())
    }


    #[inline]
    pub async fn show_spaces(&self, session_id: i64){
        let query = "show spaces;";
        let resp = self.execute(session_id, query).await.unwrap();
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
    pub async fn create_space(&self, space_name: &str, partition_num: u8, replica_factor: u8, is_fixed_string: bool, fixed_string_len: u8, comment: &str, session_id: i64){
        let query = self.get_create_space_query(space_name, partition_num, replica_factor, is_fixed_string, fixed_string_len, comment);
        // println!("{}", query);
        let _resp = self.execute(session_id, query.as_str()).await.unwrap();
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
    pub async fn create_tag_or_edge(&self, space_name: &str, col_type: ColType, tag_name: &str, comment: &str, tags: Vec<Tag>, session_id: i64){

        let query = self.get_create_tag_or_edge(space_name, col_type, tag_name, comment, tags);
        //println!("{}", query);

        let _resp = self.execute(session_id, query.as_str()).await.unwrap();

        //println!("{:?}", _resp);
    }

    #[inline]
    // INSERT VERTEX t2 (name, age) VALUES "11":("n1", 12);
    pub async fn insert_tag(&self, space_name: &str, tag_name: &str, kv: HashMap<String, String>, vid: &str, session_id: i64){

        if self.find_tag_or_edge(space_name, tag_name, ColType::Tag, session_id).await == false{
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
        let _resp = self.execute(session_id, query.as_str()).await.unwrap();
        // println!("{:?}", _resp);
    }

    #[inline]
    pub async fn insert_tags(&self, insert_tag_queries: Vec<InsertTagQuery>, session_id: i64){
        for query in insert_tag_queries{
            self.insert_tag(query.space_name.as_str(), query.tag_name.as_str(), query.kv, query.vid.as_str(), session_id).await;
        }
    }

    #[inline]
    pub async fn insert_edges(&self, insert_edge_queries: Vec<InsertEdgeQueryWithRank>, session_id: i64){
        for query in insert_edge_queries{
            self.insert_edge_with_rank(query.space_name.as_str(), query.edge_name.as_str(), query.kv, query.from_vertex.as_str(), query.to_vertex.as_str(), query.rank, session_id).await;
        }
    }

    #[inline]
    // INSERT EDGE e2 (name, age) VALUES "11"->"13":("n1", 12);
    pub async fn insert_edge(&self, space_name: &str, edge_name: &str, kv: HashMap<String, String>, from_vertex: &str, to_vertex: &str, session_id: i64){

        if self.find_tag_or_edge(space_name, edge_name, ColType::Edge, session_id).await == false{
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
        let _resp = self.execute(session_id, query.as_str()).await.unwrap();
    }


    #[inline]
    // INSERT EDGE e2 (name, age) VALUES "11"->"13"@1:("n1", 12);
    pub async fn insert_edge_with_rank(&self, space_name: &str, edge_name: &str, kv: HashMap<String, String>, from_vertex: &str, to_vertex: &str, rank: i64, session_id: i64){
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
        let _resp = self.execute(session_id, query.as_str()).await.unwrap();
        // println!("{:?}", _resp);
    }

    #[inline]
    // CREATE TAG INDEX `index_tag` on `stu`      (`name`(10), `age`) COMMENT "this is an index for tag"
    pub async fn create_index(&self, space_name: &str, index_type: ColType, tag_or_edge_name: &str, index_name: &str, comment: &str, indexed_properties: HashMap<String, u8>, session_id: i64){
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
        let _resp = self.execute(session_id, query.as_str()).await.unwrap();
        // println!("{:?}", _resp);
    }

    #[inline]
    pub async fn find_tag_or_edge(&self, space_name: &str, tag_or_edge_name: &str, col_type: ColType, session_id: i64) -> bool{
        let mut query = Self::use_space(space_name);
        match col_type {
            ColType::Edge => query += "show edges;",
            ColType::Tag => query += "show tags;",
        }
        let resp = self.execute(session_id, query.as_str()).await.unwrap();
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
