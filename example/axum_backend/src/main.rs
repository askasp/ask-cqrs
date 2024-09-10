use std::{fs::File, io::Write, path::Path, sync::Arc};

use ask_cqrs::commandhandler;
use axum::{
    http::{
        header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE},
        HeaderValue, Method, StatusCode,
    },
    routing::{get, post},
    Json, Router,
};
mod domain;
use domain::bank_stream::aggregate::BankAccountAggregate;
use domain::bank_stream::{aggregate::BankAccountError, command::BankAccountCommand};
use eventstore::Client;
use serde::{Deserialize, Serialize};
use tower_http::cors::{any, AllowHeaders, AllowOrigin, Any, CorsLayer};
use utoipa::{
    openapi::{
        self,
        security::{Http, HttpAuthScheme, SecurityScheme},
    },
    Modify, OpenApi, ToSchema,
};
mod auth;

struct AppState {
    client: Arc<Client>,
}

#[tokio::main]
async fn main() {
    // initialize tracing
    let origins: [HeaderValue; 2] = [
        HeaderValue::from_str("http://localhost:8081").unwrap(),
        HeaderValue::from_str("http://localhost:5173").unwrap(),
    ];
    let cors = CorsLayer::new()
        // allow `GET` and `POST` when accessing the resource
        .allow_methods(vec![Method::OPTIONS, Method::GET, Method::POST])
        .allow_credentials(true)
        .allow_origin(origins)
        .allow_headers([AUTHORIZATION, ACCEPT, CONTENT_TYPE]); // Exp

    let settings = "esdb://127.0.0.1:2113?tls=false&keepAliveTimeout=10000&keepAliveInterval=10000&MaxConcurrentItems=5000"
    .parse()
    .unwrap();

    let client = Arc::new(Client::new(settings).unwrap());
    let app_state = Arc::new(AppState { client });
    // allow requests from any origin

    write_openapi().await.unwrap();
    // build our application with a route
    let app = Router::new()
        // `GET /` goes to `root`
        // `POST /users` goes to `create_user`
        .route("/bank-stream", post(bank_stream::dispatch))
        .with_state(app_state)
        .layer(cors);

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

mod bank_stream {
    use std::sync::Arc;

    use ask_cqrs::{command::DomainCommand, commandhandler};
    use axum::{extract::State, http::StatusCode, Json};

    use crate::{
        auth::{AuthUser, Role},
        domain::bank_stream::{aggregate::BankAccountAggregate, command::BankAccountCommand},
        AppState,
    };
    use axum_extra::{
        headers::{authorization::Bearer, Authorization},
        TypedHeader,
    };

    // )]
    #[commandhandler(BankAccountCommand, BankAccountError, "/bank-stream")]
    #[axum::debug_handler]
    pub async fn dispatch(
        token: TypedHeader<Authorization<Bearer>>,
        State(app_state): State<Arc<AppState>>,
        Json(payload): Json<BankAccountCommand>,
    ) -> Result<StatusCode, ()> {
        println!("Token: {:?}", token);

        ask_cqrs::execute_command::<BankAccountAggregate>(
            app_state.client.clone(),
            payload.clone(),
            &payload.stream_id(),
            AuthUser {
                user_id: "user_id".to_string(),
                role: Role::User,
            }
            .into(),
            (),
        )
        .await
        .unwrap();

        // insert your application logic here

        // this will be converted into a JSON response
        // with a status code of `201 Created`
        Ok(StatusCode::CREATED)
    }
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
#[openapi(
    paths(bank_stream::dispatch),
    components(schemas(BankAccountCommand, BankAccountError))
)]
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
