/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

use nebula_rust::graph_client;
use rand::Rng;

#[tokio::main]
async fn main() {
    let address = String::from("root:root@192.168.106.129:9669/testGraph");
    
    let v:Vec<&str> = address.split('@').collect();
    let v2:Vec<&str> = v[1].split('/').collect();
    let add = String::from(v2[0]);
    let v3:Vec<&str> = v[0].split(':').collect();
    let username = String::from(v3[0]);
    let password = String::from(v3[1]);

    let mut conf = graph_client::pool_config::PoolConfig::new();
    conf.min_connection_pool_size(2)
        .max_connection_pool_size(10)
        .address(add)
        .username(username)
        .password(password);

    let pool = graph_client::connection_test::ConnectionPool::new(&conf);
    
    pool.create_new_connection().await;
    
    // pool.create_new_connections().await;

    fn get_random_string(len: usize) -> String{
        let mut rng = rand::thread_rng();
        let mut test: Vec<u8> = vec![0; len];
        for i in &mut test{
            let dig_or_char: u8 = rng.gen_range(0..=1);
            println!("{}", dig_or_char);
            match dig_or_char{
                0 => *i = rng.gen_range(48..=57),
                _ => *i = rng.gen_range(97..=122),
            }
        }
        // rng.fill_bytes(&mut test);
        // println!("{:?}", String::from_utf8(test).unwrap());
        String::from_utf8(test).unwrap()
    }
    println!("{}", get_random_string(32));


    let session = pool.get_session("root", "root", true).await.unwrap();

    // let _resp = session.execute("CREATE SPACE IF NOT EXISTS `TokenTransfer` (partition_num = 1, replica_factor = 1, vid_type = FIXED_STRING(50));").await.unwrap();
    // std::thread::sleep(std::time::Duration::from_millis(5000));
    // let _resp = session.execute("use TokenTransfer").await.unwrap();
    // // std::thread::sleep(std::time::Duration::from_millis(2000));
    // let _resp = session.execute("CREATE tag IF NOT EXISTS `transfer` (`from` string NOT NULL  , `to` string NOT NULL  , `value` int32 NOT NULL  )  ").await.unwrap();
    // std::thread::sleep(std::time::Duration::from_millis(5000));
    // let _resp = session.execute("CREATE SPACE IF NOT EXISTS `Poi$` (partition_num = 1, replica_factor = 1, vid_type = FIXED_STRING(50))").await.unwrap();
    // // std::thread::sleep(std::time::Duration::from_millis(2000));
    // let _resp = session.execute("use Poi").await.unwrap();
    // std::thread::sleep(std::time::Duration::from_millis(5000));
    // let _resp = session.execute("CREATE tag IF NOT EXISTS `transfer` (`from` string NOT NULL  , `to` string NOT NULL  , `value` int32 NOT NULL  )  ").await.unwrap();
    
    // let query = "use `TokenTransfer`; INSERT VERTEX transfer (from_account, to_account, value) VALUES \"transfer1\":(\"A\", \"B\", 12);";
    // let _resp = session.execute(query).await.unwrap();

    // let resp = session.execute("show hosts").await.unwrap();
    // println!("SHOW HOSTS: ");
    // resp.show_data();
    // println!("====================================");

    // let _resp = session.execute("use basketballplayer").await;
    // let resp = session.execute("update vertex on player 'player100' set name='Tim Duncan'").await.unwrap();
    // println!("SET NAME: "); 
    // resp.show_data();
    // println!("====================================");

    // let _resp = session.execute("use basketballplayer").await;
    // let resp = session.execute("match (n) return n limit 10").await.unwrap();
    // println!("QUERY: match (n) return n limit 10");
    // resp.show_data();

    // let _resp = session.execute("use basketballplayer").await;
    // let resp = session.execute("match ()-[r:follow]->() return r limit 10").await.unwrap();
    // println!("QUERY: match ()-[r:follow]->() return r limit 10");
    // resp.show_data();

    // // MATCH p=(n:player)-[r]->(m:player) return p limit 20
    // let _resp = session.execute("use basketballplayer").await;
    // let resp = session.execute("MATCH p=(n:player)-[r]->(m:player) return p limit 20").await.unwrap();
    // println!("QUERY: MATCH p=(n:player)-[r]->(m:player) return p limit 20");
    // resp.show_data();
}
