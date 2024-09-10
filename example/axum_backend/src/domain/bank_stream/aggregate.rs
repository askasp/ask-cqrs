use std::sync::Arc;

use ask_cqrs::command::DomainCommand;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::stream;
use thiserror::Error;
use utoipa::ToSchema;

use super::{command::BankAccountCommand, event::BankAccountEvent};

extern crate ask_cqrs;



#[derive(Debug, Default, Clone)]
pub struct BankAccountState {
    user_id: String,
    balance: i32,
    account_id: String,
}

#[derive(Debug, Error, ToSchema)]
pub enum BankAccountError {
    #[error("EventStore error: {0}")]
    EventStore(#[from] eventstore::Error),

    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Insufficient funds")]
    NotEnoughFunds,

    #[error("Account already exists")]
    AlreadyOpened,

    #[error("Account not found")]
    AccountNotFound,
}

pub struct BankAccountService {}

#[derive(ToSchema)]
pub struct BankAccountAggregate;

impl ask_cqrs::aggregate::Aggregate for BankAccountAggregate {
    type Event = BankAccountEvent;
    type Command = BankAccountCommand;
    type DomainError = BankAccountError;
    type State = BankAccountState;
    // add external servie here, can be used when executing commands, for some it can be optional, for others it might be somehthin


    fn apply_event(state: &mut Option<Self::State>, event: &Self::Event) {
        match state {
            None => match event {
                BankAccountEvent::AccountOpened {
                    user_id,
                    balance,
                    account_id,
                } => {
                    *state = Some(BankAccountState {
                        balance: *balance,
                        user_id: user_id.to_string(),
                        account_id: account_id.to_string(),
                    })
                }
                _ => panic!("Account not found"),
            },

            Some(s) => match event {
                BankAccountEvent::FundsDeposited { amount, account_id } => s.balance += amount,

                BankAccountEvent::FundsWithdrawn { amount, account_id } => s.balance -= amount,
                BankAccountEvent::OwnerChanged {
                    user_id,
                    account_id,
                } => s.user_id = user_id.clone(),
                _ => panic!("Account not found"),
            },
        }
    }

    fn execute(
        state: &Option<Self::State>,
        command: &Self::Command,
        stream_id: &str,
        service: Self::Service,
    ) -> Result<Vec<Self::Event>, Self::DomainError> {
        let event = match state {
            Some(state) => match command {
                BankAccountCommand::OpenAccount { user_id } => Err(BankAccountError::AlreadyOpened),
                BankAccountCommand::DepositFunds { amount, account_id } => {
                    Ok(BankAccountEvent::FundsDeposited {
                        amount: *amount,
                        account_id: account_id.clone(),
                    })
                }
                BankAccountCommand::ChangeOwner {
                    user_id,
                    account_id,
                } => Ok(BankAccountEvent::OwnerChanged {
                    user_id: user_id.clone(),
                    account_id: account_id.clone(),
                }),
                BankAccountCommand::WithdrawFunds { amount, account_id } => {
                    if state.balance < *amount {
                        Err(BankAccountError::NotEnoughFunds)
                    } else {
                        Ok(BankAccountEvent::FundsWithdrawn {
                            amount: *amount,
                            account_id: account_id.clone(),
                        })
                    }
                }
                BankAccountCommand::DisputeWithdrawal {
                    withdrawal_id,
                    account_id,
                } => Ok(BankAccountEvent::WithdrawalDisputed {
                    withrawal_id: withdrawal_id.clone(),
                    account_id: account_id.clone(),
                }),
            },
            None => match command {
                BankAccountCommand::OpenAccount { user_id } => {
                    Ok(BankAccountEvent::AccountOpened {
                        user_id: user_id.clone(),
                        account_id: stream_id.to_string(),
                        balance: 0,
                    })
                }
                _ => Err(BankAccountError::AccountNotFound),
            },
        }?;
        Ok(vec![event])
    }

    fn name() -> &'static str {
        "BankAccountAggregate"
    }
}
