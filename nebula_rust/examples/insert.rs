/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

use std::collections::HashMap;

use nebula_rust::graph_client::{nebula_schema::{ColType, Tag, DataType, InsertTagQuery, InsertEdgeQueryWithRank},connection_pool,session};
use rand::Rng;

#[tokio::main]
async fn main() {
   let address = "root:root@49.52.27.117:9669/testGraph";

    let pool = connection_pool::ConnectionPool_nebula::new_pool(address);
    
    pool.create_new_connection().await;

    fn get_random_string(len: usize) -> String{
        let mut rng = rand::thread_rng();
        let mut test: Vec<u8> = vec![0; len];
        for i in &mut test{
            let dig_or_char: u8 = rng.gen_range(0..=1);
            match dig_or_char{
                0 => *i = rng.gen_range(48..=57),
                _ => *i = rng.gen_range(97..=122),
            }
        }
        String::from_utf8(test).unwrap()
    }
    fn get_random_int(min: i32, max: i32) -> i32{
        let mut rng = rand::thread_rng();
        let res: i32 = rng.gen_range(min..=max);
        res
    }

    let session = pool.get_session(true).await.unwrap();

    session.show_spaces().await;
    let space_name="testGraph";
    
    //insert tags
    let mut insert_tag_queries: Vec<InsertTagQuery> = Vec::new();
    let mut addresses:Vec<String>=Vec::new();
    /*
    1~30 一般用户
    31~40 汇聚一级节点
    41~45 汇聚二级节点
    46~60 61~70 71~80 81~90 91~100发散节点
    out 配个out节点说明转出
    */
    let mut mp:HashMap<i32, i32>= HashMap::new();
    let mut countd:HashMap<i32, i32>= HashMap::new();
    for i in 1..=100{
        let address=String::from("0x")+get_random_string(40).as_str();
        println!("{} : {}",i,address);
        addresses.push(String::from(address.clone()));
        if i<=30 {
            mp.insert(i, 500);
            countd.insert(i,0);
        }else{
            mp.insert(i,0);
        }
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("id".to_string(), "\"".to_string()+address.as_str()+"\"");
        let vid=address;
        let tag_name=String::from("user");
        let insert_tag_query=InsertTagQuery::new(String::from(space_name.clone()), tag_name, properties, vid);
        insert_tag_queries.push(insert_tag_query);
    }
    let mut properties: HashMap<String, String> = HashMap::new();
    properties.insert("id".to_string(), "\"".to_string()+"out"+"\"");
    let vid=String::from("out");
    let tag_name=String::from("user");
    let insert_tag_query=InsertTagQuery::new(String::from(space_name.clone()), tag_name, properties, vid);
    insert_tag_queries.push(insert_tag_query);
    session.insert_tags(insert_tag_queries).await;

    fn get_transfer(x:i32, y:i32, mp:&mut HashMap<i32,i32>,addresses:&mut Vec<String>, space_name:&str,block:String) -> InsertEdgeQueryWithRank{
        let a=mp.get(&x).clone();
        let v=get_random_int(1, *a.unwrap());
        let mut from=mp.get_mut(&x);
        (*from.unwrap())-=v;
        let mut to=mp.get_mut(&y);
        (*to.unwrap())+=v;
        let mut properties:HashMap<String, String>=HashMap::new();
        let ax=addresses.get((x-1) as usize).unwrap().clone();
        let bx=addresses.get((y-1) as usize).unwrap().clone();
        properties.insert("from_account".to_string(),"\"".to_string()+ax.as_str()+"\"");
        properties.insert("to_account".to_string(),"\"".to_string()+bx.as_str()+"\"");
        properties.insert("value".to_string(),"\"".to_string()+v.to_string().as_str()+"\"");
        properties.insert("block".to_string(),block);
        let insert_edge_query = InsertEdgeQueryWithRank::new(
            String::from(space_name.clone()),
            "tx".to_string(),
            properties,
            ax,
            bx,
            0
        );
        insert_edge_query
    }
    fn get_multitransfer(x:i32, y:i32, z:i32,mp:&mut HashMap<i32,i32>,addresses:&mut Vec<String>, space_name:&str,block:String, s:&mut Vec<InsertEdgeQueryWithRank>){
        let a=mp.get(&x).clone();
        let v=get_random_int(1, (*a.unwrap()).clone());
        let vv=get_random_int(1, std::cmp::max(v,v-1));
        let mut from=mp.get_mut(&x).unwrap();
        (*from)-=v.clone();
        let mut to1=mp.get_mut(&y).unwrap();
        (*to1)+=(v.clone()-vv.clone());
        let mut to2=mp.get_mut(&z).unwrap();
        let outv=get_random_int(1, std::cmp::max(vv,vv-1));
        (*to2)+=(vv.clone()-outv.clone());
        let ax=addresses.get((x-1) as usize).unwrap().clone();
        let bx=addresses.get((y-1) as usize).unwrap().clone();
        let cx=addresses.get((z-1) as usize).unwrap().clone();
        
            let mut properties1:HashMap<String, String>=HashMap::new();
            properties1.insert("from_account".to_string(),"\"".to_string()+ax.clone().as_str()+"\"");
            properties1.insert("to_account".to_string(),"\"".to_string()+bx.clone().as_str()+"\"");
            properties1.insert("value".to_string(),"\"".to_string()+(v-vv).to_string().as_str()+"\"");
            properties1.insert("block".to_string(),block.clone());
            let insert_edge_query = InsertEdgeQueryWithRank::new(
                String::from(space_name.clone()),
                "tx".to_string(),
                properties1,
                ax.clone(),
                bx.clone(),
                0
            );
            s.push(insert_edge_query);
        
        
            let mut properties2:HashMap<String, String>=HashMap::new();
            properties2.insert("from_account".to_string(),"\"".to_string()+ax.clone().as_str()+"\"");
            properties2.insert("to_account".to_string(),"\"".to_string()+cx.clone().as_str()+"\"");
            properties2.insert("value".to_string(),"\"".to_string()+vv.to_string().as_str()+"\"");
            properties2.insert("block".to_string(),block.clone());
            let insert_edge_query = InsertEdgeQueryWithRank::new(
                String::from(space_name.clone()),
                "tx".to_string(),
                properties2,
                ax.clone(),
                cx.clone(),
                0
            );
            s.push(insert_edge_query);
        
        
            let mut properties3:HashMap<String, String>=HashMap::new();
            properties3.insert("from_account".to_string(),"\"".to_string()+cx.clone().as_str()+"\"");
            properties3.insert("to_account".to_string(),"\"".to_string()+"out"+"\"");
            properties3.insert("value".to_string(),"\"".to_string()+outv.to_string().as_str()+"\"");
            properties3.insert("block".to_string(),block.clone());
            let insert_edge_query = InsertEdgeQueryWithRank::new(
                String::from(space_name.clone()),
                "tx".to_string(),
                properties3,
                cx.clone(),
                String::from("out"),
                0
            );
            s.push(insert_edge_query);
        
    }
    //insert edges
    let mut insert_edge_queries: Vec<InsertEdgeQueryWithRank> = Vec::new();
    // 
    for i in 1..=20{
        let x=get_random_int(1, 30);
        let y=get_random_int(1, 30);
        let z=get_random_int(31, 40);
        if x==y{
            continue;
        }
        let a=mp.get(&x).clone();
        if *a.unwrap() <= 1{
            continue;
        }
        let c=countd.get_mut(&x).unwrap();
        if *c >=19{
            continue;
        }
        let insert_edge_query = get_transfer(x, y, &mut mp, &mut addresses, space_name, String::from("1"));
        insert_edge_queries.push(insert_edge_query);
        //let c=countd.get_mut(&x);
        *c+=1;
        if *c >=19{
            
            continue;
        }
        let insert_edge_query = get_transfer(x, z, &mut mp, &mut addresses, space_name, String::from("1"));
        *c+=1;
        insert_edge_queries.push(insert_edge_query);
    }
    // 第一阶段，一般用户到一级汇聚节点
    for i in 1..=20{
        let x=get_random_int(1, 30);
        let y=get_random_int(31, 40);
        let a=mp.get(&x).clone();
        if *a.unwrap() <= 1{
            continue;
        }
        let c=countd.get_mut(&x).unwrap();
        if *c >=19{
            continue;
        }
        let insert_edge_query = get_transfer(x, y, &mut mp, &mut addresses, space_name,String::from("2"));
        insert_edge_queries.push(insert_edge_query);
        *c+=1;
    }
    //第二阶段，汇聚一级节点转账汇聚二级节点
    for i in 1..=20{
        let x=get_random_int(31, 40);
        let y=get_random_int(41, 45);
        let a=mp.get(&x).clone();
        if *a.unwrap() <= 1{
            continue;
        }
        let mut insert_edge_query = get_transfer(x, y, &mut mp, &mut addresses, space_name, String::from("3"));
        insert_edge_queries.push(insert_edge_query);

    }
    //第三阶段，汇聚二级节点转账给发散节点
    for i in 1..=20{
        let x=get_random_int(41, 45);
        let y=get_random_int(46, 60);
        let z=get_random_int(46, 60);
        if y==z{
            continue;
        }
        let a=mp.get(&x).clone();
        if *a.unwrap() <= 1{
            continue;
        }
        get_multitransfer(x, y, z,&mut mp, &mut addresses, space_name, String::from("4"),&mut insert_edge_queries);
        let a=mp.get(&x).clone();
        if *a.unwrap() <= 1{
            continue;
        }
        get_multitransfer(x, y, z,&mut mp, &mut addresses, space_name, String::from("5"),&mut insert_edge_queries);
    }
    for i in 1..20{
        let x=get_random_int(46, 60);
        let y=get_random_int(61, 70);
        let z=get_random_int(61, 70);
        if y==z || x==y || x==z{
            continue;
        }
        let a=mp.get(&x).clone();
        if *a.unwrap() <= 1{
            continue;
        }
        get_multitransfer(x, y, z,&mut mp, &mut addresses, space_name, String::from("6"),&mut insert_edge_queries);
    }
    for i in 1..10{
        let x=get_random_int(61, 70);
        let y=get_random_int(71, 80);
        let z=get_random_int(71, 80);
        if y==z || x==y || x==z{
            continue;
        }
        let a=mp.get(&x).clone();
        if *a.unwrap() <= 1{
            continue;
        }
        get_multitransfer(x, y, z,&mut mp, &mut addresses, space_name, String::from("7"),&mut insert_edge_queries);
    }
    for i in 1..10{
        let x=get_random_int(71, 80);
        let y=get_random_int(81, 90);
        let z=get_random_int(81, 90);
        if y==z || x==y || x==z{
            continue;
        }
        let a=mp.get(&x).clone();
        if *a.unwrap() <= 1{
            continue;
        }
        get_multitransfer(x, y, z,&mut mp, &mut addresses, space_name, String::from("8"),&mut insert_edge_queries);
    }
    for i in 1..10{
        let x=get_random_int(81, 90);
        let y=get_random_int(91, 100);
        let z=get_random_int(91, 100);
        if y==z || x==y || x==z{
            continue;
        }
        let a=mp.get(&x).clone();
        if *a.unwrap() <= 1{
            continue;
        }
        get_multitransfer(x, y, z,&mut mp, &mut addresses, space_name, String::from("9"),&mut insert_edge_queries);
    }
    
    session.insert_edges(insert_edge_queries).await;
}
