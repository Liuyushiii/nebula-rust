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

    let resp = session.execute("show spaces").await.unwrap();
    assert!(resp.error_code == common::types::ErrorCode::SUCCEEDED);
    println!("SHOW SPACES: ");
    // data: Option<dataSet>
    // dataSet: column_names, rows
    // rows: Vec<Row>
    // Row: values
    // values: Vec<Value>
    /* 
    for row in resp.data.unwrap().rows{
        let values = row.values;
        // let res = String::from_utf8(values);
        for value in values{
            if let common::types::Value::sVal(vec)= value {
                let res = String::from_utf8(vec).unwrap();
                println!("{}", res);
            }
        }
        // println!("{:?}",values);
    }
    */
    resp.show_data();
    println!("==================");

    let resp = session.execute("show hosts").await.unwrap();
    println!("SHOW HOSTS: ");
    // println!("{:?}", resp);
    resp.show_data();
    // println!("{:?}", resp.data.as_ref().unwrap());
}
