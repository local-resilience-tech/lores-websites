use serde::Serialize;

#[derive(Serialize)]
pub struct WebsiteCreatedDataV1 {
    pub name: String,
    pub description: String,
}

#[derive(Serialize)]
pub enum AppOperation {
    WebsiteCreatedV1(WebsiteCreatedDataV1),
}
