use clap::{command, Parser};

use crate::state::MasterConfig;

pub struct Config {
    pub master_config: Option<MasterConfig>,
    pub replicaof: bool,
    pub port: u16,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Port of the redis server
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    /// a Redis server assumes the "master" role. When the --replicaof flag is passed, the server assumes the "slave" role instead
    /// --replicaof <MASTER_HOST> <MASTER_PORT>
    #[clap(short, long, number_of_values = 2)]
    replicaof: Option<Vec<String>>,
}

pub fn load_config() -> anyhow::Result<Config> {
    let args = Args::parse();
    let master_config = if let Some(replicaof) = &args.replicaof {
        let host = replicaof.first().unwrap().to_owned();
        let port: usize = replicaof.last().unwrap().parse()?;
        Some(MasterConfig { host, port })
    } else {
        None
    };

    Ok(Config {
        master_config,
        replicaof: args.replicaof.is_some(),
        port: args.port,
    })
}
