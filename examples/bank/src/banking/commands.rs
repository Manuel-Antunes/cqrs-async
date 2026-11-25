//! The bank's commands, and the two gateways handling them: one per
//! aggregate root, each committing to its own log.

use cqrs::{
    aggregate::{AggregateLoader, New},
    command::{CommandContext, CommandHandler},
    store::InMemoryEventStore,
};

use super::domain::{Account, AccountEvent, BankError, Customer, CustomerEvent};

/// Makes someone a customer, under number `id`.
#[cqrs::command(error = BankError)]
#[derive(Clone, Debug)]
pub struct RegisterCustomer {
    /// Their number.
    pub id: u32,
    /// Their name.
    pub name: String,
}

/// Changes a customer's name.
#[cqrs::command(error = BankError)]
#[derive(Clone, Debug)]
pub struct RenameCustomer {
    /// Their number.
    pub id: u32,
    /// Their new name.
    pub name: String,
}

/// Opens account `id` for customer `customer`.
#[cqrs::command(error = BankError)]
#[derive(Clone, Debug)]
pub struct OpenAccount {
    /// The account's number.
    pub id: u32,
    /// Its owner, a registered customer.
    pub customer: u32,
}

/// Puts money in an account.
#[cqrs::command(error = BankError)]
#[derive(Clone, Debug)]
pub struct Deposit {
    /// The account.
    pub account: u32,
    /// How much, in cents.
    pub amount: u64,
}

/// Takes money out of an account.
#[cqrs::command(error = BankError)]
#[derive(Clone, Debug)]
pub struct Withdraw {
    /// The account.
    pub account: u32,
    /// How much, in cents.
    pub amount: u64,
}

/// The customers' commands, committed to the customers' log through `P`
/// (the customers' events service).
#[cqrs::command_gateway(commands = CustomerCommands, handles(RegisterCustomer, RenameCustomer))]
pub struct CustomerGateway<P> {
    /// Loads customers from their log.
    pub customers: AggregateLoader<Customer, InMemoryEventStore<CustomerEvent>>,
    /// The unit of work: appends to the customers' log, then announces.
    #[gateway(unit)]
    pub events: P,
}

#[cqrs::command_handler]
impl<P> CommandHandler<RegisterCustomer> for CustomerGateway<P> {
    async fn handle(
        &self,
        command: &RegisterCustomer,
        ctx: &mut CommandContext<CustomerEvent>,
    ) -> Result<(), BankError> {
        Customer::register(&command.name, ctx)?;
        ctx.record(New::<Customer>(command.id));
        Ok(())
    }
}

#[cqrs::command_handler]
impl<P> CommandHandler<RenameCustomer> for CustomerGateway<P> {
    async fn handle(&self, command: &RenameCustomer, ctx: &mut CommandContext<CustomerEvent>) -> Result<(), BankError> {
        let customer = self
            .customers
            .load(&command.id)
            .await?
            .ok_or(BankError::NoSuchCustomer(command.id))?;
        customer.rename(&command.name, ctx)?;
        ctx.record(customer);
        Ok(())
    }
}

/// The accounts' commands, committed to the accounts' log through `P`.
/// Opening an account reads the customers' log: the owner must exist.
#[cqrs::command_gateway(commands = AccountCommands, handles(OpenAccount, Deposit, Withdraw))]
pub struct AccountGateway<P> {
    /// Loads customers, to check an account's owner.
    pub customers: AggregateLoader<Customer, InMemoryEventStore<CustomerEvent>>,
    /// Loads accounts from their log.
    pub accounts: AggregateLoader<Account, InMemoryEventStore<AccountEvent>>,
    /// The unit of work: appends to the accounts' log, then announces.
    #[gateway(unit)]
    pub events: P,
}

#[cqrs::command_handler]
impl<P> CommandHandler<OpenAccount> for AccountGateway<P> {
    async fn handle(&self, command: &OpenAccount, ctx: &mut CommandContext<AccountEvent>) -> Result<(), BankError> {
        if self.customers.load(&command.customer).await?.is_none() {
            return Err(BankError::NoSuchCustomer(command.customer));
        }
        Account::open(command.customer, ctx);
        ctx.record(New::<Account>(command.id));
        Ok(())
    }
}

#[cqrs::command_handler]
impl<P> CommandHandler<Deposit> for AccountGateway<P> {
    async fn handle(&self, command: &Deposit, ctx: &mut CommandContext<AccountEvent>) -> Result<(), BankError> {
        let account = self.load(command.account).await?;
        account.deposit(command.amount, ctx)?;
        ctx.record(account);
        Ok(())
    }
}

#[cqrs::command_handler]
impl<P> CommandHandler<Withdraw> for AccountGateway<P> {
    async fn handle(&self, command: &Withdraw, ctx: &mut CommandContext<AccountEvent>) -> Result<(), BankError> {
        let account = self.load(command.account).await?;
        account.withdraw(command.amount, ctx)?;
        ctx.record(account);
        Ok(())
    }
}

impl<P> AccountGateway<P> {
    /// Account `id`, as of its last event.
    async fn load(&self, id: u32) -> Result<cqrs::aggregate::Loaded<Account>, BankError> {
        self.accounts.load(&id).await?.ok_or(BankError::NoSuchAccount(id))
    }
}
