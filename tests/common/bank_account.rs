use ask_cqrs::{aggregate::Aggregate, command::DomainCommand};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BankAccountCommand {
    #[serde(flatten)]
    pub command: BankAccountCommandType,
    #[serde(skip)]
    pub account_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BankAccountCommandType {
    OpenAccount { user_id: String },
    DepositFunds { amount: u64, account_id: String },
    WithdrawFunds { amount: u64, account_id: String },
    SuspendAccount { account_id: String },
}

#[typetag::serde]
impl DomainCommand for BankAccountCommand {
    fn stream_id(&self) -> String {
        match &self.command {
            BankAccountCommandType::OpenAccount { .. } => {
                self.account_id.clone().unwrap_or_else(|| Uuid::new_v4().to_string())
            }
            BankAccountCommandType::DepositFunds { account_id, .. } => account_id.clone(),
            BankAccountCommandType::WithdrawFunds { account_id, .. } => account_id.clone(),
            BankAccountCommandType::SuspendAccount { account_id } => account_id.clone(),
        }
    }
}

impl BankAccountCommand {
    pub fn open_account(user_id: String) -> Self {
        Self {
            command: BankAccountCommandType::OpenAccount { user_id },
            account_id: None,
        }
    }

    pub fn deposit_funds(amount: u64, account_id: String) -> Self {
        Self {
            command: BankAccountCommandType::DepositFunds { amount, account_id },
            account_id: None,
        }
    }

    pub fn withdraw_funds(amount: u64, account_id: String) -> Self {
        Self {
            command: BankAccountCommandType::WithdrawFunds { amount, account_id },
            account_id: None,
        }
    }

    pub fn suspend_account(account_id: String) -> Self {
        Self {
            command: BankAccountCommandType::SuspendAccount { account_id },
            account_id: None,
        }
    }

    pub fn with_account_id(mut self, account_id: String) -> Self {
        self.account_id = Some(account_id);
        self
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
        match &command.command {
            BankAccountCommandType::OpenAccount { user_id } => {
                if state.is_some() {
                    return Err(BankAccountError::AccountAlreadyExists);
                }
                Ok(vec![BankAccountEvent::AccountOpened {
                    user_id: user_id.clone(),
                    account_id: stream_id.to_string(),
                }])
            }
            BankAccountCommandType::DepositFunds { amount, account_id } => {
                if state.is_none() {
                    return Err(BankAccountError::AccountNotFound);
                }
                Ok(vec![BankAccountEvent::FundsDeposited {
                    amount: *amount,
                    account_id: account_id.clone(),
                }])
            }
            BankAccountCommandType::WithdrawFunds { amount, account_id } => {
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
            BankAccountCommandType::SuspendAccount { account_id } => {
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