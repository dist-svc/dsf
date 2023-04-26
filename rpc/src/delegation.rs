use clap::Parser;

use crate::ServiceIdentifier;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub enum DelegationCommands {
    #[clap(name = "datastore")]
    /// Request a signed hello / discovery payload
    Hello,

    /// Register a service
    Register(Page),


}
