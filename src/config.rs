use std::env;

use crate::state::MasterConfig;

pub struct Config {
    pub master_config: Option<MasterConfig>,
    pub replicaof: bool,
    pub port: String,
}

pub fn load_config() -> anyhow::Result<Config> {
    let mut port = None;
    let mut replicaof = false;
    let mut master_host = None;
    let mut master_port = None;
    let args: Vec<String> = env::args().collect();

    for i in 1..args.len() {
        match args[i].as_str() {
            "--port" | "-p" => {
                if i + 1 < args.len() {
                    port = Some(args[i + 1].clone());
                }
            }
            "--replicaof" => {
                if i + 2 < args.len() {
                    master_host = Some(args[i + 1].clone());
                    master_port = Some(args[i + 2].parse().clone()?);
                }
                replicaof = true;
            }

            _ => (),
        }
    }
    let port = match port {
        Some(p) => p,
        None => "6379".to_string(),
    };

    let master_config = if let (Some(host), Some(port)) = (master_host, master_port) {
        Some(MasterConfig { host, port })
    } else {
        None
    };

    Ok(Config {
        master_config,
        replicaof,
        port,
    })
}
