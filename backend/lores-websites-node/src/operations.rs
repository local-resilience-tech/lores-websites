use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct WebsiteCreatedDataV1 {
    pub name: String,
    pub description: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum AppOperation {
    WebsiteCreatedV1(WebsiteCreatedDataV1),
}
