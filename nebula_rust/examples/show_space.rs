/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

use nebula_rust::graph_client;

#[tokio::main]
async fn main() {
    let mut conf = graph_client::pool_config::PoolConfig::new();
    conf.min_connection_pool_size(2)
        .max_connection_pool_size(10)
        .address("localhost:9669".to_string());

    let pool = graph_client::connection_pool::ConnectionPool::new(&conf).await;
    let session = pool.get_session("root", "nebula", true).await.unwrap();

    // data: Option<dataSet>
    // dataSet: column_names, rows
    // rows: Vec<Row>
    // Row: values
    // values: Vec<Value>>
    
    /* 
    let resp = session.execute("show spaces").await.unwrap();
    println!("SHOW SPACES: ");
    resp.show_data();
    println!("====================================");

    let resp = session.execute("show hosts").await.unwrap();
    println!("SHOW HOSTS: ");
    resp.show_data();
    println!("====================================");

    let _resp = session.execute("use basketballplayer").await;
    let resp = session.execute("update vertex on player 'player100' set name='Tim Duncan'").await.unwrap();
    println!("SET NAME: "); 
    resp.show_data();
    println!("====================================");

    let _resp = session.execute("use basketballplayer").await;
    let resp = session.execute("match (n) return n limit 10").await.unwrap();
    println!("QUERY: match (n) return n limit 10");
    resp.show_data();

    let _resp = session.execute("use basketballplayer").await;
    let resp = session.execute("match ()-[r:follow]->() return r limit 10").await.unwrap();
    println!("QUERY: match ()-[r:follow]->() return r limit 10");
    resp.show_data();

    // MATCH p=(n:player)-[r]->(m:player) return p limit 20
    let _resp = session.execute("use basketballplayer").await;
    let resp = session.execute("MATCH p=(n:player)-[r]->(m:player) return p limit 20").await.unwrap();
    println!("QUERY: MATCH p=(n:player)-[r]->(m:player) return p limit 20");
    resp.show_data();

    */
}
