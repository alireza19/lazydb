use crate::event::{AppEvent, Event, EventHandler};
use clap::Parser;
use ratatui::{
    crossterm::event::{KeyCode, KeyEvent, KeyModifiers},
    DefaultTerminal,
};
use sqlx::PgPool;
use std::env;
use tracing::info;

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

/// Current view state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CurrentView {
    /// Showing connection status (connecting or failed).
    ConnectionStatus,
    /// Showing table list after successful connection.
    TableList,
}

/// Application state.
pub struct App {
    /// Is the application running?
    pub running: bool,
    /// Database connection state.
    pub connection: ConnectionState,
    /// Current view.
    pub current_view: CurrentView,
    /// List of tables in public schema.
    pub tables: Vec<String>,
    /// Currently selected table index.
    pub selected_table_index: usize,
    /// Event handler.
    pub events: EventHandler,
}

impl std::fmt::Debug for App {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("App")
            .field("running", &self.running)
            .field("connection", &self.connection)
            .field("current_view", &self.current_view)
            .field("tables", &self.tables)
            .field("selected_table_index", &self.selected_table_index)
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
            current_view: CurrentView::ConnectionStatus,
            tables: Vec::new(),
            selected_table_index: 0,
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
                    // Spawn task to fetch tables
                    let sender = self.events.sender();
                    let pool_clone = pool.clone();
                    tokio::spawn(async move {
                        let tables = fetch_tables(&pool_clone).await;
                        let _ = sender.send(Event::App(AppEvent::TablesLoaded(tables)));
                    });

                    self.connection = ConnectionState::Connected { pool, db_name };
                    self.current_view = CurrentView::TableList;
                }
                Err(error) => {
                    self.connection = ConnectionState::Failed { error };
                    self.current_view = CurrentView::ConnectionStatus;
                }
            },
            AppEvent::TablesLoaded(tables) => {
                self.tables = tables;
                self.selected_table_index = 0;
            }
        }
    }

    /// Handles the key events and updates the state of [`App`].
    pub fn handle_key_events(&mut self, key_event: KeyEvent) -> color_eyre::Result<()> {
        match key_event.code {
            KeyCode::Esc | KeyCode::Char('q') => self.events.send(AppEvent::Quit),
            KeyCode::Char('c' | 'C') if key_event.modifiers == KeyModifiers::CONTROL => {
                self.events.send(AppEvent::Quit)
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.current_view == CurrentView::TableList && !self.tables.is_empty() {
                    if self.selected_table_index > 0 {
                        self.selected_table_index -= 1;
                    } else {
                        self.selected_table_index = self.tables.len() - 1;
                    }
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.current_view == CurrentView::TableList && !self.tables.is_empty() {
                    if self.selected_table_index < self.tables.len() - 1 {
                        self.selected_table_index += 1;
                    } else {
                        self.selected_table_index = 0;
                    }
                }
            }
            KeyCode::Enter => {
                if self.current_view == CurrentView::TableList && !self.tables.is_empty() {
                    let table_name = &self.tables[self.selected_table_index];
                    info!("Selected: {}", table_name);
                }
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

/// Fetch all tables in the public schema.
async fn fetch_tables(pool: &PgPool) -> Vec<String> {
    let result: Result<Vec<(String,)>, _> = sqlx::query_as(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename",
    )
    .fetch_all(pool)
    .await;

    match result {
        Ok(rows) => rows.into_iter().map(|(name,)| name).collect(),
        Err(e) => {
            tracing::error!("Failed to fetch tables: {e}");
            Vec::new()
        }
    }
}
