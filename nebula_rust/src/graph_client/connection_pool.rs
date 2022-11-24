use crate::graph_client::connection::Connection;
use crate::graph_client::pool_config::PoolConfig;
use crate::graph_client::session::Session;

/// The pool of connection to server, it's MT-safe to access.
/// 与服务器的连接池，它是 MT 安全访问的。
pub struct ConnectionPool_nebula {
    /// The connections
    /// The interior mutable to enable could get multiple sessions in one scope
    /// 内部可变启用可以在一个范围内获得多个会话
    conns: std::sync::Mutex<std::cell::RefCell<std::collections::LinkedList<Connection>>>,
    /// It should be immutable
    /// 它应该是不可变的
    config: PoolConfig,
    /// Address cursor
    /// 地址光标
    cursor: std::sync::Mutex<usize>,
    /// The total count of connections, contains which hold by session
    /// 连接总数，包含会话持有的连接数
    conns_count: std::sync::Mutex<usize>,
}

impl ConnectionPool_nebula {
    /// Construct pool by the configuration
    pub fn new(conf: &PoolConfig) -> Self {

        // println!("conf: {:?}", conf.clone());

        let conns = std::collections::LinkedList::<Connection>::new();
        let pool = ConnectionPool_nebula {
            conns: std::sync::Mutex::new(std::cell::RefCell::new(conns)),
            config: conf.clone(),
            cursor: std::sync::Mutex::new(0),
            conns_count: std::sync::Mutex::new(0),
        };
        assert!(pool.config.min_connection_pool_size <= pool.config.max_connection_pool_size);
        // pool.new_connection(pool.config.min_connection_pool_size).await;
        pool
    }

    // 创建一个新的connection，但是不在这个方法里连接nebula
    pub fn new_pool(nebula_url: &str) -> Self{
        let v:Vec<&str> = nebula_url.split('@').collect();
        let v2:Vec<&str> = v[1].split('/').collect();
        let add = String::from(v2[0]);
        let v3:Vec<&str> = v[0].split(':').collect();
        let username = String::from(v3[0]);
        let password = String::from(v3[1]);

        let mut conf = PoolConfig::new();
        conf.min_connection_pool_size(2)
            .max_connection_pool_size(10)
            .address(add)
            .set_username(username)
            .set_password(password);
        let pool = ConnectionPool_nebula::new(&conf);
        // pool.create_new_connection().await;
        pool
    }


    pub async fn create_new_connection(&self){
        self.new_connection(self.config.min_connection_pool_size).await;
    }

    /// Get a session authenticated by username and password
    /// retry_connect means keep the connection available if true
    /// 获取由用户名和密码验证的会话 retry_connect 表示如果为真则保持连接可用
    pub async fn get_session(
        &self,
        // username: &str,
        // password: &str,
        retry_connect: bool,
    ) -> std::result::Result<Session<'_>, common::types::ErrorCode> {

        let username = self.config.username.clone();
        let password = self.config.password.clone();


        // println!("==========getSession=============");
        if self.conns.lock().unwrap().borrow_mut().is_empty() {
            self.new_connection(1).await;
        }
        let conn = self.conns.lock().unwrap().borrow_mut().pop_back();
        if let Some(conn) = conn {
            // get authentication with username and password
            let resp = conn.authenticate(username.as_str(), password.as_str()).await?;
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

    /// Get the count of connections
    #[inline]
    pub fn len(&self) -> usize {
        self.conns.lock().unwrap().borrow().len()
    }

    /// Give back the connection to pool
    #[inline]
    pub fn give_back(&self, conn: Connection) {
        self.conns.lock().unwrap().borrow_mut().push_back(conn);
    }

    pub async fn new_connection(&self, inc: u32) {
        assert!(inc != 0);
        // TODO concurrent these
        let mut count = 0;
        let mut loop_count = 0;
        let loop_limit = inc as usize * self.config.addresses.len();
        while count < inc {

            let conns_count_mutex: usize;

            {
                conns_count_mutex = * self.conns_count.lock().unwrap();
            }

            if count as usize
                + conns_count_mutex
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
        
        let mut conns_count_add = self.conns_count.lock().unwrap();
        * conns_count_add += count as usize;

        // self.conns_count
        //     .borrow_mut()
        //     .fetch_add(count as usize, std::sync::atomic::Ordering::Release);
    }


        // cursor on the server addresses
    // 服务器地址上的光标
    fn cursor(&self) -> usize {
        // println!("current cursor: {:?}", self.cursor.borrow().load(std::sync::atomic::Ordering::Relaxed));

        // println!("current config.addresses.len(): {:?}", self.config.addresses.len());
        let cursor_mutex: usize ;

        {
            cursor_mutex = * self.cursor.lock().unwrap();
        }

        if cursor_mutex  >= self.config.addresses.len()
        {
            {
                let mut a = self.cursor.lock().unwrap();
                * a = 0;
            }
                // println!("return 0");
            0
        } else {
            // println!("return cursor");

            let b: usize;

            {
                b = * self.cursor.lock().unwrap();
            }
            {
                let mut cursor_add = self.cursor.lock().unwrap();
                * cursor_add += 1 as usize;
            }
            
            b
        }
    }

    pub fn get_config(&self){
        println!("{:?}", self.config);
    }
}