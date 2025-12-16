use crate::app::{App, Cli};
use clap::Parser;
use crossterm::{
    event::{DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture},
    execute,
};
use std::io::stdout;

pub mod app;
pub mod dotline;
pub mod event;
pub mod ui;

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let cli = Cli::parse();
    let database_url = cli.get_database_url()?;

    let terminal = ratatui::init();

    // Enable bracketed paste mode for instant paste handling
    // Enable mouse capture for scroll wheel support
    execute!(stdout(), EnableBracketedPaste, EnableMouseCapture)?;

    let result = App::new(database_url).run(terminal).await;

    // Disable mouse capture and bracketed paste mode before restoring terminal
    let _ = execute!(stdout(), DisableMouseCapture, DisableBracketedPaste);
    ratatui::restore();

    result
}
