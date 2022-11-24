/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

use nebula_rust::graph_client;
use rand::Rng;

#[tokio::main]
async fn main() {
    let address = "root:root@49.52.27.117:9669/testGraph";
    
    let conf_nebula = graph_client::pool_config::PoolConfig::new_conf(address);

    let conn_nebula = graph_client::connection::Connection::new_from_conf(&conf_nebula).await.unwrap();

    let resp = conn_nebula.authenticate(conf_nebula.username.clone().as_str(), conf_nebula.password.clone().as_str()).await.unwrap();

    let session_id = resp.session_id.unwrap();

    // let resp = conn_nebula.execute(session_id, "use TokenTransfer;go from \"0x0016eccecffc25b94050187017eb59fa05c029aa\" OVER tx YIELD properties(edge);").await.unwrap();
    let resp = conn_nebula.execute(session_id, "use testGraph;go 3 to 3 steps from \"0xh12rvhmxo22cszk73krl02vr82k6frfn0klk6ron\" OVER tx YIELD properties(edge);").await.unwrap();
    
    // let ans = resp.into_json_with_name(resp.parse_resp().unwrap(),"edge".to_string());

    // println!("{}", ans);
}
