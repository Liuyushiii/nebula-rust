use crate::graph_client::connection::Connection;
use crate::graph_client::pool_config::PoolConfig;
use crate::graph_client::session::Session;
use crate::graph_client::connection_pool::ConnectionPool;

pub struct Pool{
    
    connection_pool: ConnectionPool,

}

impl  Pool {
    pub fn new(
        connection_pool: ConnectionPool
    ) -> Self{
        Pool { connection_pool: connection_pool }
    }
}



