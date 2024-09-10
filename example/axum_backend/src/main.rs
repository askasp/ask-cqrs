use std::{fs::File, io::Write, path::Path};

use ask_cqrs::commandhandler;
use axum::{
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
mod domain;
use domain::bank_stream::{aggregate::BankAccountError, command::BankAccountCommand};
use domain::bank_stream::aggregate::BankAccountAggregate;
use serde::{Deserialize, Serialize};
use utoipa::{openapi::{self, security::{Http, HttpAuthScheme, SecurityScheme}}, Modify, OpenApi, ToSchema};

#[tokio::main]
async fn main() {
    // initialize tracing

    write_openapi().await.unwrap();
    // build our application with a route
    let app = Router::new()
        // `GET /` goes to `root`
        // `POST /users` goes to `create_user`
        .route("/users", post(handle_command));

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// basic handler that responds with a static string

// #[axum::debug_handler]
// #[utoipa::path(
//     post,
//     path = "bank-stream",
//     request_body = BankAccountCommand,
//     responses(
//         (status = 200, description = "command dispatched successfully"),
//         (status = 403, description = "Unauthorized"),
//         (status = 422, description = "Domain validation error", body = BankAccountError)
//     ),
//     params(
//         ("Authorization" = Option<String>, Header, description = "bearer token"),
//       ),
//     security(
//         ("bearerAuth" = [])
//     )

    
// )]
#[commandhandler(BankAccountCommand, BankAccountError, "bank-stream")]
#[axum::debug_handler]
async fn handle_command(Json(payload): Json<BankAccountCommand>) -> StatusCode {
    // insert your application logic here

    // this will be converted into a JSON response
    // with a status code of `201 Created`
    StatusCode::CREATED
}

#[axum::debug_handler]
async fn create_user(
    // this argument tells axum to parse the request body
    // as JSON into a `CreateUser` type
    Json(payload): Json<bool>,
) -> StatusCode {
    // insert your application logic here
    // this will be converted into a JSON response
    // with a status code of `201 Created`
    StatusCode::CREATED 
}

#[derive(OpenApi)]
#[openapi(paths(handle_command), components(schemas(BankAccountCommand, BankAccountError)))]
struct ApiDoc;
struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components: &mut utoipa::openapi::Components = openapi.components.as_mut().unwrap(); // we can unwrap safely since there already is components registered.
        components.add_security_scheme(
            "bearerAuth",
            SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
        )
    }
}

pub async fn write_openapi() -> std::io::Result<()> {
    let json = ApiDoc::openapi().to_pretty_json().unwrap();
    let dest_path = Path::new(".").join("openapi.json");
    let mut f = File::create(&dest_path).unwrap();

    // event!(Level::INFO, "Writing to openapi.json");
    f.write_all(json.as_bytes())
}
