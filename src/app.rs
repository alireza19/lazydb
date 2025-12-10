use crate::event::{AppEvent, Event, EventHandler};
use clap::Parser;
use ratatui::{
    crossterm::event::{KeyCode, KeyEvent, KeyModifiers},
    DefaultTerminal,
};
use sqlx::PgPool;
use std::env;

/// A lazydocker-inspired database TUI
#[derive(Parser, Debug)]
#[command(name = "lazydb")]
#[command(about = "A lazydocker-inspired database TUI written in Rust")]
pub struct Cli {
    /// Database connection URL (can also be set via DATABASE_URL env var)
    #[arg(long = "url", short = 'u')]
    pub database_url: Option<String>,
}

impl Cli {
    /// Get the database URL from CLI arg or environment variable.
    pub fn get_database_url(&self) -> color_eyre::Result<String> {
        if let Some(url) = &self.database_url {
            return Ok(url.clone());
        }

        env::var("DATABASE_URL")
            .map_err(|_| color_eyre::eyre::eyre!("DATABASE_URL not set. Provide --url or set DATABASE_URL environment variable."))
    }
}

/// Database connection state.
#[derive(Debug)]
pub enum ConnectionState {
    /// Currently attempting to connect.
    Connecting,
    /// Successfully connected.
    Connected { pool: PgPool, db_name: String },
    /// Connection failed.
    Failed { error: String },
}

/// Application state.
pub struct App {
    /// Is the application running?
    pub running: bool,
    /// Database connection state.
    pub connection: ConnectionState,
    /// Event handler.
    pub events: EventHandler,
}

impl std::fmt::Debug for App {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("App")
            .field("running", &self.running)
            .field("connection", &self.connection)
            .finish()
    }
}

impl App {
    /// Constructs a new instance of [`App`] and spawns the connection task.
    pub fn new(database_url: String) -> Self {
        let events = EventHandler::new();
        let sender = events.sender();

        // Spawn the connection task
        tokio::spawn(async move {
            let result = connect_to_database(&database_url).await;
            let _ = sender.send(Event::App(AppEvent::ConnectionResult(result)));
        });

        Self {
            running: true,
            connection: ConnectionState::Connecting,
            events,
        }
    }

    /// Run the application's main loop.
    pub async fn run(mut self, mut terminal: DefaultTerminal) -> color_eyre::Result<()> {
        while self.running {
            terminal.draw(|frame| frame.render_widget(&self, frame.area()))?;
            match self.events.next().await? {
                Event::Tick => self.tick(),
                Event::Crossterm(event) => match event {
                    crossterm::event::Event::Key(key_event)
                        if key_event.kind == crossterm::event::KeyEventKind::Press =>
                    {
                        self.handle_key_events(key_event)?
                    }
                    _ => {}
                },
                Event::App(app_event) => self.handle_app_event(app_event),
            }
        }
        Ok(())
    }

    /// Handle application-specific events.
    fn handle_app_event(&mut self, event: AppEvent) {
        match event {
            AppEvent::Quit => self.quit(),
            AppEvent::ConnectionResult(result) => match result {
                Ok((pool, db_name)) => {
                    self.connection = ConnectionState::Connected { pool, db_name };
                }
                Err(error) => {
                    self.connection = ConnectionState::Failed { error };
                }
            },
        }
    }

    /// Handles the key events and updates the state of [`App`].
    pub fn handle_key_events(&mut self, key_event: KeyEvent) -> color_eyre::Result<()> {
        match key_event.code {
            KeyCode::Esc | KeyCode::Char('q') => self.events.send(AppEvent::Quit),
            KeyCode::Char('c' | 'C') if key_event.modifiers == KeyModifiers::CONTROL => {
                self.events.send(AppEvent::Quit)
            }
            _ => {}
        }
        Ok(())
    }

    /// Handles the tick event of the terminal.
    pub fn tick(&self) {}

    /// Set running to false to quit the application.
    pub fn quit(&mut self) {
        self.running = false;
    }
}

/// Attempt to connect to the database and fetch the current database name.
async fn connect_to_database(url: &str) -> Result<(PgPool, String), String> {
    let pool = PgPool::connect(url)
        .await
        .map_err(|e| format!("{e}"))?;

    let db_name: (String,) = sqlx::query_as("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .map_err(|e| format!("Connected but failed to query database name: {e}"))?;

    Ok((pool, db_name.0))
}
