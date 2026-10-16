use std::io;
use std::process::ExitCode;

use std::path::PathBuf;

use rs_arrow_ipc_strings2redb::SimpleConfig;

fn io_envkey2val(key: &'static str) -> impl Fn() -> String {
    move || std::env::var(key).unwrap_or_default()
}

fn io_table_name() -> impl Fn() -> String {
    io_envkey2val("ENV_TABLE_NAME")
}

fn io_redb_out_path() -> impl Fn() -> String {
    io_envkey2val("ENV_OUT_REDB_PATH")
}

fn io_config() -> impl Fn() -> SimpleConfig {
    || {
        let table_name: String = io_table_name()();
        let redb_out_path: PathBuf = io_redb_out_path()().into();
        SimpleConfig {
            table_name,
            redb_out_path,
        }
    }
}

fn io_main() -> impl FnOnce() -> Result<(), io::Error> {
    || {
        let cfg: SimpleConfig = io_config()();
        cfg.stdin2ipc2redb()
    }
}

fn sub() -> Result<(), io::Error> {
    io_main()()
}

fn main() -> ExitCode {
    sub().map(|_| ExitCode::SUCCESS).unwrap_or_else(|e| {
        eprintln!("{e}");

        let cfg: SimpleConfig = io_config()();

        let SimpleConfig {
            table_name,
            redb_out_path,
        } = cfg;

        eprintln!("ENV_TABLE_NAME: {table_name}");
        eprintln!("ENV_OUT_REDB_PATH: {redb_out_path:#?}");

        ExitCode::FAILURE
    })
}
