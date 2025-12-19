use crate::app::{App, Cli};
use clap::Parser;
use crossterm::{
    event::{DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture},
    execute,
};
use std::io::stdout;
use tracing_subscriber::prelude::*;
use tui_logger::{TuiTracingSubscriberLayer, init_logger, set_default_level};

pub mod app;
pub mod dotline;
pub mod event;
pub mod ui;

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    // Initialize tui-logger for the DB logs pane
    init_logger(tui_logger::LevelFilter::Trace).expect("failed to init tui-logger");
    set_default_level(tui_logger::LevelFilter::Trace);

    // Set up tracing to route to tui-logger
    tracing_subscriber::registry()
        .with(TuiTracingSubscriberLayer)
        .init();

    let cli = Cli::parse();
    let database_url = cli.get_database_url()?;

    let terminal = ratatui::init();
    execute!(stdout(), EnableBracketedPaste, EnableMouseCapture)?;

    let result = App::new(database_url).run(terminal).await;

    let _ = execute!(stdout(), DisableMouseCapture, DisableBracketedPaste);
    ratatui::restore();

    result
}
