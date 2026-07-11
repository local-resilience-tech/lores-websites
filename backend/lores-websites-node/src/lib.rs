use lores_app_node::AppNode;

use crate::operations::AppOperation;

pub mod operations;

pub type LoresWebsiteNode = AppNode<AppOperation>;

pub fn connect(
    grpc_addr: String,
    app_id: impl Into<String>,
    instance_id: impl Into<String>,
) -> LoresWebsiteNode {
    AppNode::grpc(grpc_addr, app_id, instance_id)
}
