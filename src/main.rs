use crate::app::{App, Cli};
use clap::Parser;

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
    let result = App::new(database_url).run(terminal).await;
    ratatui::restore();

    result
}
