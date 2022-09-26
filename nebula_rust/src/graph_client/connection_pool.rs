/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

use crate::graph_client::connection::Connection;
use crate::graph_client::pool_config::PoolConfig;
use crate::graph_client::session::Session;

/// The pool of connection to server, it's MT-safe to access.
/// 与服务器的连接池，它是 MT 安全访问的。
pub struct ConnectionPool {
    /// The connections
    /// The interior mutable to enable could get multiple sessions in one scope
    /// 内部可变启用可以在一个范围内获得多个会话
    conns: std::sync::Mutex<std::cell::RefCell<std::collections::LinkedList<Connection>>>,
    /// It should be immutable
    /// 它应该是不可变的
    config: PoolConfig,
    /// Address cursor
    /// 地址光标
    cursor: std::cell::RefCell<std::sync::atomic::AtomicUsize>,
    /// The total count of connections, contains which hold by session
    /// 连接总数，包含会话持有的连接数
    conns_count: std::cell::RefCell<std::sync::atomic::AtomicUsize>,
}

impl ConnectionPool {
    /// Construct pool by the configuration
    pub async fn new(conf: &PoolConfig) -> Self {

        println!("conf: {:?}", conf.clone());

        let conns = std::collections::LinkedList::<Connection>::new();
        let pool = ConnectionPool {
            conns: std::sync::Mutex::new(std::cell::RefCell::new(conns)),
            config: conf.clone(),
            cursor: std::cell::RefCell::new(std::sync::atomic::AtomicUsize::new(0)),
            conns_count: std::cell::RefCell::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        assert!(pool.config.min_connection_pool_size <= pool.config.max_connection_pool_size);
        pool.new_connection(pool.config.min_connection_pool_size)
            .await;
        pool
    }

    /// Get a session authenticated by username and password
    /// retry_connect means keep the connection available if true
    /// 获取由用户名和密码验证的会话 retry_connect 表示如果为真则保持连接可用
    pub async fn get_session(
        &self,
        username: &str,
        password: &str,
        retry_connect: bool,
    ) -> std::result::Result<Session<'_>, common::types::ErrorCode> {
        if self.conns.lock().unwrap().borrow_mut().is_empty() {
            self.new_connection(1).await;
        }
        let conn = self.conns.lock().unwrap().borrow_mut().pop_back();
        if let Some(conn) = conn {
            // get authentication with username and password
            let resp = conn.authenticate(username, password).await?;
            if resp.error_code != common::types::ErrorCode::SUCCEEDED {
                return Err(resp.error_code);
            }
            Ok(Session::new(
                resp.session_id.unwrap(),
                conn,
                self,
                username.to_string(),
                password.to_string(),
                if let Some(time_zone_name) = resp.time_zone_name {
                    std::str::from_utf8(&time_zone_name).unwrap().to_string()
                } else {
                    String::new()
                },
                resp.time_zone_offset_seconds.unwrap(),
                retry_connect,
            ))
        } else {
            Err(common::types::ErrorCode::E_UNKNOWN)
        }
    }

    /// Give back the connection to pool
    #[inline]
    pub fn give_back(&self, conn: Connection) {
        self.conns.lock().unwrap().borrow_mut().push_back(conn);
    }

    /// Get the count of connections
    #[inline]
    pub fn len(&self) -> usize {
        self.conns.lock().unwrap().borrow().len()
    }

    // Add new connection to pool
    // inc is the count of new connection created, which shouldn't be zero
    // the incremental count maybe can't fit when occurs error in connection creating
    // 将新连接添加到池
    // inc 是创建的新连接的计数，不应为零，增量计数可能不适合在连接创建中发生错误时
    async fn new_connection(&self, inc: u32) {
        assert!(inc != 0);
        // TODO concurrent these
        let mut count = 0;
        let mut loop_count = 0;
        let loop_limit = inc as usize * self.config.addresses.len();
        while count < inc {
            if count as usize
                + self
                    .conns_count
                    .borrow()
                    .load(std::sync::atomic::Ordering::Acquire)
                >= self.config.max_connection_pool_size as usize
            {
                // Reach the pool size limit
                break;
            }
            let cursor = { self.cursor() };
            match Connection::new_from_address(&self.config.addresses[cursor]).await {
                Ok(conn) => {
                    // append the conn to the conenction list
                    self.conns.lock().unwrap().borrow_mut().push_back(conn);
                    count += 1;
                }
                Err(_) => (),
            };
            loop_count += 1;
            if loop_count > loop_limit {
                // Can't get so many connections, avoid dead loop
                break;
            }
        }
        // Release ordering make sure inc happened after creating new connections
        // 发布订单确保在创建新连接后发生 inc
        self.conns_count
            .borrow_mut()
            .fetch_add(count as usize, std::sync::atomic::Ordering::Release);
    }

    // cursor on the server addresses
    // 服务器地址上的光标
    fn cursor(&self) -> usize {
        // println!("current cursor: {:?}", self.cursor.borrow().load(std::sync::atomic::Ordering::Relaxed));

        // println!("current config.addresses.len(): {:?}", self.config.addresses.len());
        if self
            .cursor
            .borrow()
            .load(std::sync::atomic::Ordering::Relaxed)
            >= self.config.addresses.len()
        {
            self.cursor
                .borrow_mut()
                .store(0, std::sync::atomic::Ordering::Relaxed);
                println!("return 0");
            0
        } else {
            println!("return cursor");
            self.cursor
                .borrow_mut()
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        }
    }
}
