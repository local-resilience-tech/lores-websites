use serde::Serialize;

#[derive(Clone, Serialize)]
pub struct WebsiteCreatedDataV1 {
    pub name: String,
    pub description: String,
}

#[derive(Clone, Serialize)]
pub enum AppOperation {
    WebsiteCreatedV1(WebsiteCreatedDataV1),
}
