use lores_app_node::AppNode;

use crate::operations::AppOperation;

pub mod operations;

pub type LoresWebsiteNode = AppNode<AppOperation>;

pub fn connect(grpc_addr: String, region_id: [u8; 32], namespace: impl Into<String>) -> LoresWebsiteNode {
    AppNode::grpc(grpc_addr, region_id, namespace)
}
