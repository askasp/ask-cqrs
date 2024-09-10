use ask_cqrs::command::DomainCommand;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[serde(tag = "event")]
pub enum BankAccountCommand {
    OpenAccount {
        user_id: String,
    },
    DepositFunds {
        amount: i32,
        account_id: String,
    },
    WithdrawFunds {
        amount: i32,
        account_id: String,
    },
    ChangeOwner {
        user_id: String,
        account_id: String,
    },
    DisputeWithdrawal {
        withdrawal_id: String,
        account_id: String,
    },
    // Define your command structure
}

#[typetag::serde]
impl DomainCommand for BankAccountCommand {
    fn stream_id(&self) -> String {
        match self {
            BankAccountCommand::OpenAccount { user_id } => Uuid::new_v4().to_string(),
            BankAccountCommand::DepositFunds { account_id, .. } => account_id.to_owned(),
            BankAccountCommand::WithdrawFunds { account_id, .. } => account_id.to_owned(),
            BankAccountCommand::ChangeOwner { account_id, .. } => account_id.to_owned(),
            BankAccountCommand::DisputeWithdrawal { account_id, .. } => account_id.to_owned(),
        }
    }
}
