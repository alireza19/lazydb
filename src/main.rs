use crate::app::{App, Cli};
use clap::Parser;
use crossterm::{event::{DisableBracketedPaste, EnableBracketedPaste}, execute};
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
    execute!(stdout(), EnableBracketedPaste)?;

    let result = App::new(database_url).run(terminal).await;

    // Disable bracketed paste mode before restoring terminal
    let _ = execute!(stdout(), DisableBracketedPaste);
    ratatui::restore();

    result
}
