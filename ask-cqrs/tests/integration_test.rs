use ask_cqrs::aggregate::{self};
use ask_cqrs::command::DomainCommand;
use ask_cqrs::event_handler::EventHandler;
use ask_cqrs::execute_command;
// use ask_cqrs::read_model_old;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use common::bank_account_view::BankAccountView;
use common::bank_aggregate::{BankAccountAggregate, BankAccountCommand};
// use ask_cqrs::{execute_command, start_aggregate};
// use bank_aggregate::{BankAccountAggregate, BankAccountCommand};
use axum::extract::Json;
use common::fraud_detection_handler::{FraudDetectionHandler, FraudDetectionService};
use core::time;
use eventstore::Client;
use http::HeaderMap;
use ractor::{call_t, Actor, ActorRef};
use std::time::Duration;
use std::{
    sync::{Arc, Once},
    thread,
};
use tokio::time::{sleep, Sleep};
use tracing::{event, instrument};
use uuid::Uuid;
mod common;

extern crate ask_cqrs;
static INIT: Once = Once::new();
struct Aggregates {
    bank_aggregate: BankAccountAggregate,
}

fn create_es_client() -> Arc<Client> {
    let settings = "esdb://127.0.0.1:2113?tls=false&keepAliveTimeout=10000&keepAliveInterval=10000&MaxConcurrentItems=5000"
        .parse()
        .unwrap();

    let client = Arc::new(Client::new(settings).unwrap());
    client
}
// Initialize the logger only once

fn initialize_logger() {
    INIT.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive("ask_cqrs=debug".parse().unwrap()) // Set DEBUG for your package
                    .add_directive("warn".parse().unwrap()), // Set WARN for all other packages
            )
            .init();
    });
}

#[tokio::test]
async fn test_aggregate_happy_path() -> Result<(), anyhow::Error> {
    initialize_logger();

    let user1_id = Uuid::new_v4().to_string();
    let command = BankAccountCommand::OpenAccount {
        user_id: user1_id.clone(),
    };
    let stream_id = command.stream_id();

    let command_2 = BankAccountCommand::DepositFunds {
        amount: 100,
        account_id: stream_id.clone(),
    };

    let client: Arc<Client> = create_es_client();
    ask_cqrs::execute_command::<BankAccountAggregate>(client.clone(), command, &stream_id, (), ())
        .await?;
    ask_cqrs::execute_command::<BankAccountAggregate>(
        client.clone(),
        command_2,
        &stream_id,
        (),
        (),
    )
    .await?;

    Ok(())
}
#[tokio::test]
async fn test_aggregate_cant_deposit_non_existend() -> Result<(), anyhow::Error> {
    initialize_logger();

    let command_2 = BankAccountCommand::DepositFunds {
        amount: 100,
        account_id: "dont exists".to_string(),
    };

    let client: Arc<Client> = create_es_client();
    let res = ask_cqrs::execute_command::<BankAccountAggregate>(
        client,
        command_2.clone(),
        command_2.clone().stream_id().as_str(),
        (),
        (),
    )
    .await;

    match res {
        Ok(()) => Err(anyhow::anyhow!(
            "Should not be able to deposit to non existing account"
        )),
        Err(e) => Ok(()),
    }
}

#[tokio::test]
#[instrument]
async fn test_read_model_happy_path() -> Result<(), anyhow::Error> {
    initialize_logger();

    tracing::info!("Bank view about to start");
    let client = create_es_client();
    let bank_view = ask_cqrs::start_view_builder::<BankAccountView>(client.clone())
        .await
        .unwrap();

    tracing::info!("Bank view started");
    let user1_id = Uuid::new_v4().to_string();
    let command = BankAccountCommand::OpenAccount {
        user_id: user1_id.clone(),
    };
    let stream_id = command.stream_id();

    let command_2 = BankAccountCommand::DepositFunds {
        amount: 100,
        account_id: stream_id.clone(),
    };

    ask_cqrs::execute_command::<BankAccountAggregate>(client.clone(), command, &stream_id, (), ())
        .await?;
    ask_cqrs::execute_command::<BankAccountAggregate>(
        client.clone(),
        command_2,
        &stream_id,
        (),
        (),
    )
    .await?;

    let account = bank_view.lookup(&stream_id).unwrap();
    sleep(Duration::from_millis(100)).await;
    assert!(account.balance == 100);

    let open_command = BankAccountCommand::OpenAccount {
        user_id: user1_id.clone(),
    };

    let stream_id = open_command.stream_id();
    ask_cqrs::execute_command::<BankAccountAggregate>(
        client.clone(),
        open_command,
        &stream_id,
        (),
        (),
    )
    .await?;
    let user_accounts = bank_view.lookup_by_tag(&user1_id);

    assert!(user_accounts.len() == 2);

    Ok(())
}

#[tokio::test]
#[instrument]
async fn test_fraud_detection_handler() -> Result<(), anyhow::Error> {
    initialize_logger();

    let client = create_es_client();
    let bank_view = ask_cqrs::start_view_builder::<BankAccountView>(client.clone())
        .await
        .unwrap();

    let fraud_service = FraudDetectionService {
        bank_view: bank_view.clone(),
        reqwest_client: Arc::new(reqwest::Client::new()),
        es_client: client.clone(),
    };
    ask_cqrs::start_event_handler::<FraudDetectionHandler>(client.clone(), fraud_service).await;

    tracing::info!("Bank view started");
    let user1_id = Uuid::new_v4().to_string();
    let command = BankAccountCommand::OpenAccount {
        user_id: user1_id.clone(),
    };
    let stream_id = command.stream_id();
    let command_2 = BankAccountCommand::DepositFunds {
        amount: 5000,
        account_id: stream_id.clone(),
    };
    let command_3 = BankAccountCommand::WithdrawFunds {
        amount: 2000,
        account_id: stream_id.clone(),
    };
    {};

    ask_cqrs::execute_command::<BankAccountAggregate>(client.clone(), command, &stream_id, (), ())
        .await?;
    ask_cqrs::execute_command::<BankAccountAggregate>(
        client.clone(),
        command_2,
        &stream_id,
        (),
        (),
    )
    .await?;
    ask_cqrs::execute_command::<BankAccountAggregate>(
        client.clone(),
        command_3,
        &stream_id,
        (),
        (),
    )
    .await?;

    sleep(Duration::from_secs(1)).await;

    Ok(())
}
