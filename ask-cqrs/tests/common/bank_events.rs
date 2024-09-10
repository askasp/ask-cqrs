use ask_cqrs::event::DomainEvent;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BankAccountEvent {
    AccountOpened {
        user_id: String,
        balance: i32,
        account_id: String,
    },
    OwnerChanged {
        user_id: String,
        account_id: String,
    },
    FundsDeposited {
        amount: i32,
        account_id: String,
    },
    FundsWithdrawn {
        amount: i32,
        account_id: String,
    },
    WithdrawalDisputed {
        withrawal_id: String,
        account_id: String,
    },
}
#[typetag::serde]
impl DomainEvent for BankAccountEvent {
    fn stream_id(&self) -> String {
        match self {
            BankAccountEvent::AccountOpened { account_id, .. } => account_id.clone(),
            BankAccountEvent::OwnerChanged { account_id, .. } => account_id.clone(),
            BankAccountEvent::FundsDeposited { account_id, .. } => account_id.clone(),
            BankAccountEvent::FundsWithdrawn { account_id, .. } => account_id.clone(),
            BankAccountEvent::WithdrawalDisputed { account_id, .. } => account_id.clone(),
        }
    }
    // fn as_any(&self) -> &dyn std::any::Any {
        // self
    // }
}
