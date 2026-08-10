//! `monosend` - a small LocalSend command-line client.

use color_eyre::{
    Result,
    eyre::{WrapErr, eyre},
};
use std::ffi::OsString;
use std::path::PathBuf;

mod events;
mod localsend;
mod receive;
mod share;
mod theme;
mod utils;

use localsend::protocol::LOCALSEND_DEFAULT_PORT;

const HELP: &str = r#"monosend - send and receive files with LocalSend

Usage:
  monosend receive [--port PORT] [--autoaccept]
  monosend share [--clipboard] <FILE>...

Commands:
  receive    Listen for incoming LocalSend transfers
  share      Open a device picker and send files

Receive options:
  --port PORT       HTTPS port to listen on (default: 53317)
  --autoaccept      Accept transfers without asking
  --autoaceept      Alias for --autoaccept (kept for compatibility)

Share options:
  --clipboard       Add the current text clipboard as clipboard.txt

Run `monosend receive --help` or `monosend share --help` for command-specific help.
"#;

const RECEIVE_HELP: &str = r#"Usage: monosend receive [OPTIONS]

Listen for LocalSend transfer requests and save accepted files in the current
directory. By default, each request is shown through notify-send with Accept
and Decline actions. Successfully received .txt files are also copied to the
desktop clipboard.

Options:
  --port PORT       HTTPS port to listen on (default: 53317)
  --autoaccept      Accept transfers automatically
  --autoaceept      Alias for --autoaccept
  -h, --help        Print help
"#;

const SHARE_HELP: &str = r#"Usage: monosend share [OPTIONS] <FILE>...

Open a focused terminal device picker, discover nearby LocalSend devices, and
send the supplied files after a receiver is selected.

Options:
  --clipboard       Add the current text clipboard as clipboard.txt
  -h, --help        Print help

Examples:
  monosend share file.txt file2.txt
  monosend share --clipboard
"#;

enum CliCommand {
    Receive {
        port: u16,
        autoaccept: bool,
    },
    Share {
        paths: Vec<PathBuf>,
        clipboard: bool,
    },
    Help(&'static str),
    Version,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let _ = rustls::crypto::ring::default_provider().install_default();

    match parse_cli(std::env::args_os().skip(1))? {
        CliCommand::Receive { port, autoaccept } => receive::run(port, autoaccept).await,
        CliCommand::Share { paths, clipboard } => share::run(paths, clipboard).await,
        CliCommand::Help(help) => {
            print!("{help}");
            Ok(())
        }
        CliCommand::Version => {
            println!("monosend {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
    }
}

fn parse_cli(args: impl Iterator<Item = OsString>) -> Result<CliCommand> {
    let mut args = args.peekable();
    let Some(command) = args.next() else {
        return Ok(CliCommand::Help(HELP));
    };

    match command.to_str() {
        Some("receive") => parse_receive_args(args),
        Some("share") => parse_share_args(args),
        Some("help") | Some("-h") | Some("--help") => Ok(CliCommand::Help(HELP)),
        Some("-V") | Some("--version") => Ok(CliCommand::Version),
        Some(other) => Err(eyre!("unknown command `{other}`\n\n{HELP}")),
        None => Err(eyre!("command must be valid UTF-8")),
    }
}

fn parse_receive_args(mut args: impl Iterator<Item = OsString>) -> Result<CliCommand> {
    let mut port = LOCALSEND_DEFAULT_PORT;
    let mut autoaccept = false;

    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--port") => {
                let value = args
                    .next()
                    .ok_or_else(|| eyre!("--port requires a port number"))?;
                port = value
                    .to_str()
                    .ok_or_else(|| eyre!("port must be valid UTF-8"))?
                    .parse::<u16>()
                    .wrap_err("port must be a number from 1 to 65535")?;
                if port == 0 {
                    return Err(eyre!("port must be a number from 1 to 65535"));
                }
            }
            Some(value) if value.starts_with("--port=") => {
                port = value["--port=".len()..]
                    .parse::<u16>()
                    .wrap_err("port must be a number from 1 to 65535")?;
                if port == 0 {
                    return Err(eyre!("port must be a number from 1 to 65535"));
                }
            }
            Some("--autoaccept" | "--autoaceept") => autoaccept = true,
            Some("-h" | "--help") => return Ok(CliCommand::Help(RECEIVE_HELP)),
            Some(other) => return Err(eyre!("unknown receive option `{other}`\n\n{RECEIVE_HELP}")),
            None => return Err(eyre!("receive options must be valid UTF-8")),
        }
    }

    Ok(CliCommand::Receive { port, autoaccept })
}

fn parse_share_args(args: impl Iterator<Item = OsString>) -> Result<CliCommand> {
    let mut paths = Vec::new();
    let mut clipboard = false;
    let mut positional_only = false;

    for arg in args {
        if !positional_only {
            match arg.to_str() {
                Some("--") => {
                    positional_only = true;
                    continue;
                }
                Some("--clipboard") => {
                    clipboard = true;
                    continue;
                }
                Some("-h" | "--help") => return Ok(CliCommand::Help(SHARE_HELP)),
                Some(value) if value.starts_with('-') => {
                    return Err(eyre!("unknown share option `{value}`\n\n{SHARE_HELP}"));
                }
                _ => {}
            }
        }
        paths.push(PathBuf::from(arg));
    }

    if paths.is_empty() && !clipboard {
        return Err(eyre!(
            "share needs at least one file or --clipboard\n\n{SHARE_HELP}"
        ));
    }

    Ok(CliCommand::Share { paths, clipboard })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_both_autoaccept_spellings() {
        for spelling in ["--autoaccept", "--autoaceept"] {
            let command = parse_cli(["receive", spelling].into_iter().map(OsString::from)).unwrap();
            assert!(matches!(
                command,
                CliCommand::Receive {
                    port: LOCALSEND_DEFAULT_PORT,
                    autoaccept: true
                }
            ));
        }
    }

    #[test]
    fn parses_share_files_and_clipboard() {
        let command = parse_cli(
            ["share", "--clipboard", "one.txt", "two.txt"]
                .into_iter()
                .map(OsString::from),
        )
        .unwrap();

        match command {
            CliCommand::Share { paths, clipboard } => {
                assert!(clipboard);
                assert_eq!(paths, [PathBuf::from("one.txt"), PathBuf::from("two.txt")]);
            }
            _ => panic!("expected share command"),
        }
    }
}
