use ask_cqrs::{aggregate::Aggregate, command::DomainCommand};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BankAccountCommand {
    OpenAccount { 
        user_id: String,
        #[serde(skip)]
        account_id: Option<String>,
    },
    DepositFunds { 
        amount: u64, 
        account_id: String,
    },
    WithdrawFunds { 
        amount: u64, 
        account_id: String,
    },
    SuspendAccount { 
        account_id: String,
    },
}

#[typetag::serde]
impl DomainCommand for BankAccountCommand {
    fn stream_id(&self) -> String {
        match self {
            BankAccountCommand::OpenAccount { account_id, .. } => {
                account_id.clone().unwrap_or_else(|| Uuid::new_v4().to_string())
            }
            BankAccountCommand::DepositFunds { account_id, .. } => account_id.clone(),
            BankAccountCommand::WithdrawFunds { account_id, .. } => account_id.clone(),
            BankAccountCommand::SuspendAccount { account_id } => account_id.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BankAccountEvent {
    AccountOpened { user_id: String, account_id: String },
    FundsDeposited { amount: u64, account_id: String },
    FundsWithdrawn { amount: u64, account_id: String },
    AccountSuspended { account_id: String },
}

impl BankAccountEvent {
    pub fn account_id(&self) -> String {
        match self {
            BankAccountEvent::AccountOpened { account_id, .. } => account_id.clone(),
            BankAccountEvent::FundsDeposited { account_id, .. } => account_id.clone(),
            BankAccountEvent::FundsWithdrawn { account_id, .. } => account_id.clone(),
            BankAccountEvent::AccountSuspended { account_id } => account_id.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BankAccountState {
    pub user_id: String,
    pub balance: u64,
    pub suspended: bool,
}

#[derive(Error, Debug)]
pub enum BankAccountError {
    #[error("Account already exists")]
    AccountAlreadyExists,
    #[error("Account not found")]
    AccountNotFound,
    #[error("Insufficient funds")]
    InsufficientFunds,
    #[error("Account is suspended")]
    AccountSuspended,
}

pub struct BankAccountAggregate;

#[async_trait]
impl Aggregate for BankAccountAggregate {
    type Event = BankAccountEvent;
    type Command = BankAccountCommand;
    type DomainError = BankAccountError;
    type State = BankAccountState;
    type Service = ();

    fn name() -> &'static str {
        "bank_account"
    }

    fn apply_event(state: &mut Option<Self::State>, event: &Self::Event) {
        match event {
            BankAccountEvent::AccountOpened { user_id, .. } => {
                *state = Some(BankAccountState {
                    user_id: user_id.clone(),
                    balance: 0,
                    suspended: false,
                });
            }
            BankAccountEvent::FundsDeposited { amount, .. } => {
                if let Some(state) = state {
                    state.balance += amount;
                }
            }
            BankAccountEvent::FundsWithdrawn { amount, .. } => {
                if let Some(state) = state {
                    state.balance -= amount;
                }
            }
            BankAccountEvent::AccountSuspended { .. } => {
                if let Some(state) = state {
                    state.suspended = true;
                }
            }
        }
    }

    fn execute(
        state: &Option<Self::State>,
        command: &Self::Command,
        stream_id: &str,
        _service: Self::Service,
    ) -> Result<Vec<Self::Event>, Self::DomainError> {
        match command {
            BankAccountCommand::OpenAccount { user_id, .. } => {
                if state.is_some() {
                    return Err(BankAccountError::AccountAlreadyExists);
                }
                Ok(vec![BankAccountEvent::AccountOpened {
                    user_id: user_id.clone(),
                    account_id: stream_id.to_string(),
                }])
            }
            BankAccountCommand::DepositFunds { amount, account_id } => {
                if state.is_none() {
                    return Err(BankAccountError::AccountNotFound);
                }
                Ok(vec![BankAccountEvent::FundsDeposited {
                    amount: *amount,
                    account_id: account_id.clone(),
                }])
            }
            BankAccountCommand::WithdrawFunds { amount, account_id } => {
                let state = state.as_ref().ok_or(BankAccountError::AccountNotFound)?;
                if state.suspended {
                    return Err(BankAccountError::AccountSuspended);
                }
                if state.balance < *amount {
                    return Err(BankAccountError::InsufficientFunds);
                }
                Ok(vec![BankAccountEvent::FundsWithdrawn {
                    amount: *amount,
                    account_id: account_id.clone(),
                }])
            }
            BankAccountCommand::SuspendAccount { account_id } => {
                if state.is_none() {
                    return Err(BankAccountError::AccountNotFound);
                }
                Ok(vec![BankAccountEvent::AccountSuspended {
                    account_id: account_id.clone(),
                }])
            }
        }
    }
} 