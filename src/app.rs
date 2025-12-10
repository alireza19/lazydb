use crate::event::{AppEvent, Event, EventHandler, TableDataResult};
use clap::Parser;
use ratatui::{
    crossterm::event::{KeyCode, KeyEvent, KeyModifiers},
    DefaultTerminal,
};
use sqlx::{Column, PgPool, Row};
use std::env;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, info};

/// Page size for table data pagination.
pub const PAGE_SIZE: usize = 50;

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

/// State for viewing a table's data.
#[derive(Debug, Clone)]
pub struct TableViewState {
    /// Name of the table being viewed.
    pub table_name: String,
    /// Column names.
    pub columns: Vec<String>,
    /// Current page of rows.
    pub rows: Vec<Vec<String>>,
    /// Total row count in the table.
    pub total_count: i64,
    /// Current page number (0-indexed).
    pub page: usize,
    /// Currently selected row index within the page.
    pub selected_row: usize,
    /// Loading state.
    pub loading: bool,
    /// Error message if fetch failed.
    pub error: Option<String>,
}

impl TableViewState {
    /// Calculate total number of pages.
    pub fn total_pages(&self) -> usize {
        if self.total_count == 0 {
            1
        } else {
            (self.total_count as usize).div_ceil(PAGE_SIZE)
        }
    }
}

/// Current view state.
#[derive(Debug, Clone)]
pub enum CurrentView {
    /// Showing connection status (connecting or failed).
    ConnectionStatus,
    /// Showing table list after successful connection.
    TableList,
    /// Viewing a specific table's data.
    TableView(TableViewState),
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
    /// Currently selected table index in sidebar.
    pub selected_table_index: usize,
    /// Event handler.
    pub events: EventHandler,
    /// Handle for the background refresh task.
    refresh_handle: Option<JoinHandle<()>>,
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
            refresh_handle: None,
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
                    // Spawn task to fetch tables (initial fetch)
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
            AppEvent::TablesLoaded(new_tables) => {
                // Only update if tables have changed
                if self.tables != new_tables {
                    // Try to preserve selection by table name
                    let previously_selected = self
                        .tables
                        .get(self.selected_table_index)
                        .cloned();

                    self.tables = new_tables;

                    // Find the previously selected table in the new list
                    if let Some(prev_name) = previously_selected {
                        if let Some(new_index) = self.tables.iter().position(|t| t == &prev_name) {
                            self.selected_table_index = new_index;
                        } else {
                            self.selected_table_index = 0;
                        }
                    } else {
                        self.selected_table_index = 0;
                    }

                    if self.tables.is_empty() {
                        self.selected_table_index = 0;
                    }

                    debug!("refreshed table list – {} tables", self.tables.len());
                }

                // Start the refresh task if not already running
                if self.refresh_handle.is_none() {
                    self.start_refresh_task();
                }
            }
            AppEvent::TableDataLoaded(result) => {
                match result {
                    Ok(data) => {
                        // Only update if we're still viewing the same table
                        if let CurrentView::TableView(ref mut state) = self.current_view
                            && state.table_name == data.table_name
                            && state.page == data.page
                        {
                            state.columns = data.columns;
                            state.rows = data.rows;
                            state.total_count = data.total_count;
                            state.loading = false;
                            state.error = None;
                            // Reset selection if out of bounds
                            if state.selected_row >= state.rows.len() && !state.rows.is_empty() {
                                state.selected_row = state.rows.len() - 1;
                            }
                        }
                    }
                    Err(error) => {
                        if let CurrentView::TableView(ref mut state) = self.current_view {
                            state.loading = false;
                            state.error = Some(error);
                        }
                    }
                }
            }
        }
    }

    /// Start the background refresh task.
    fn start_refresh_task(&mut self) {
        if let ConnectionState::Connected { pool, .. } = &self.connection {
            let pool = pool.clone();
            let sender = self.events.sender();

            let handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(3));
                interval.tick().await;

                loop {
                    interval.tick().await;

                    if sender.is_closed() {
                        debug!("refresh task stopping – channel closed");
                        break;
                    }

                    if pool.is_closed() {
                        debug!("refresh task stopping – pool closed");
                        break;
                    }

                    let tables = fetch_tables(&pool).await;
                    if sender.send(Event::App(AppEvent::TablesLoaded(tables))).is_err() {
                        break;
                    }
                }
            });

            self.refresh_handle = Some(handle);
        }
    }

    /// Fetch table data for a given table and page.
    fn fetch_table_data(&self, table_name: &str, page: usize) {
        if let ConnectionState::Connected { pool, .. } = &self.connection {
            let pool = pool.clone();
            let sender = self.events.sender();
            let table_name = table_name.to_string();

            tokio::spawn(async move {
                let result = fetch_table_page(&pool, &table_name, page).await;
                let _ = sender.send(Event::App(AppEvent::TableDataLoaded(result)));
            });
        }
    }

    /// Open a table for viewing.
    fn open_table(&mut self, table_name: &str) {
        info!("Opening table: {}", table_name);

        let state = TableViewState {
            table_name: table_name.to_string(),
            columns: Vec::new(),
            rows: Vec::new(),
            total_count: 0,
            page: 0,
            selected_row: 0,
            loading: true,
            error: None,
        };

        self.current_view = CurrentView::TableView(state);
        self.fetch_table_data(table_name, 0);
    }

    /// Handles the key events and updates the state of [`App`].
    pub fn handle_key_events(&mut self, key_event: KeyEvent) -> color_eyre::Result<()> {
        // Global quit shortcuts
        if matches!(key_event.code, KeyCode::Char('c') | KeyCode::Char('C'))
            && key_event.modifiers == KeyModifiers::CONTROL
        {
            self.events.send(AppEvent::Quit);
            return Ok(());
        }

        // Track if we need to fetch new page data
        let mut fetch_page: Option<(String, usize)> = None;

        match &mut self.current_view {
            CurrentView::ConnectionStatus => {
                if matches!(key_event.code, KeyCode::Esc | KeyCode::Char('q')) {
                    self.events.send(AppEvent::Quit);
                }
            }
            CurrentView::TableList => {
                match key_event.code {
                    KeyCode::Esc | KeyCode::Char('q') => self.events.send(AppEvent::Quit),
                    KeyCode::Up | KeyCode::Char('k') => {
                        if !self.tables.is_empty() {
                            if self.selected_table_index > 0 {
                                self.selected_table_index -= 1;
                            } else {
                                self.selected_table_index = self.tables.len() - 1;
                            }
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if !self.tables.is_empty() {
                            if self.selected_table_index < self.tables.len() - 1 {
                                self.selected_table_index += 1;
                            } else {
                                self.selected_table_index = 0;
                            }
                        }
                    }
                    KeyCode::Enter => {
                        if !self.tables.is_empty() {
                            let table_name = self.tables[self.selected_table_index].clone();
                            self.open_table(&table_name);
                        }
                    }
                    _ => {}
                }
            }
            CurrentView::TableView(state) => {
                match key_event.code {
                    KeyCode::Esc | KeyCode::Char('b') => {
                        // Go back to table list
                        self.current_view = CurrentView::TableList;
                    }
                    KeyCode::Char('q') => self.events.send(AppEvent::Quit),
                    KeyCode::Up | KeyCode::Char('k') => {
                        if !state.rows.is_empty() {
                            if state.selected_row > 0 {
                                state.selected_row -= 1;
                            } else {
                                state.selected_row = state.rows.len() - 1;
                            }
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if !state.rows.is_empty() {
                            if state.selected_row < state.rows.len() - 1 {
                                state.selected_row += 1;
                            } else {
                                state.selected_row = 0;
                            }
                        }
                    }
                    KeyCode::Left | KeyCode::Char('h') => {
                        // Previous page
                        if state.page > 0 && !state.loading {
                            state.page -= 1;
                            state.loading = true;
                            state.selected_row = 0;
                            fetch_page = Some((state.table_name.clone(), state.page));
                        }
                    }
                    KeyCode::Right | KeyCode::Char('l') => {
                        // Next page
                        let total_pages = state.total_pages();
                        if state.page < total_pages.saturating_sub(1) && !state.loading {
                            state.page += 1;
                            state.loading = true;
                            state.selected_row = 0;
                            fetch_page = Some((state.table_name.clone(), state.page));
                        }
                    }
                    _ => {}
                }
            }
        }

        // Fetch page data outside the match to avoid borrow issues
        if let Some((table_name, page)) = fetch_page {
            self.fetch_table_data(&table_name, page);
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

impl Drop for App {
    fn drop(&mut self) {
        if let Some(handle) = self.refresh_handle.take() {
            handle.abort();
        }
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

/// Fetch a page of data from a table.
async fn fetch_table_page(
    pool: &PgPool,
    table_name: &str,
    page: usize,
) -> Result<TableDataResult, String> {
    let offset = page * PAGE_SIZE;

    // Fetch total count
    let count_query = format!(r#"SELECT COUNT(*) FROM "{}""#, table_name);
    let total_count: (i64,) = sqlx::query_as(&count_query)
        .fetch_one(pool)
        .await
        .map_err(|e| format!("Failed to get row count: {e}"))?;

    // Fetch rows with columns
    let data_query = format!(
        r#"SELECT * FROM "{}" LIMIT {} OFFSET {}"#,
        table_name, PAGE_SIZE, offset
    );

    let rows = sqlx::query(&data_query)
        .fetch_all(pool)
        .await
        .map_err(|e| format!("Failed to fetch data: {e}"))?;

    // Extract column names from the first row or query metadata
    let columns: Vec<String> = if rows.is_empty() {
        // If no rows, we need to get columns from table info
        let columns_query = format!(
            r#"SELECT column_name FROM information_schema.columns 
               WHERE table_schema = 'public' AND table_name = '{}' 
               ORDER BY ordinal_position"#,
            table_name
        );
        let col_rows: Vec<(String,)> = sqlx::query_as(&columns_query)
            .fetch_all(pool)
            .await
            .map_err(|e| format!("Failed to get column info: {e}"))?;
        col_rows.into_iter().map(|(name,)| name).collect()
    } else {
        rows[0]
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect()
    };

    // Convert rows to Vec<Vec<String>>
    let string_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    // Try to get value as string, handle various types
                    row.try_get::<String, _>(i)
                        .or_else(|_| row.try_get::<i64, _>(i).map(|v| v.to_string()))
                        .or_else(|_| row.try_get::<i32, _>(i).map(|v| v.to_string()))
                        .or_else(|_| row.try_get::<f64, _>(i).map(|v| v.to_string()))
                        .or_else(|_| row.try_get::<bool, _>(i).map(|v| v.to_string()))
                        .or_else(|_| {
                            row.try_get::<Option<String>, _>(i)
                                .map(|v| v.unwrap_or_else(|| "NULL".to_string()))
                        })
                        .or_else(|_| {
                            row.try_get::<Option<i64>, _>(i)
                                .map(|v| v.map_or("NULL".to_string(), |n| n.to_string()))
                        })
                        .or_else(|_| {
                            row.try_get::<Option<i32>, _>(i)
                                .map(|v| v.map_or("NULL".to_string(), |n| n.to_string()))
                        })
                        .or_else(|_| {
                            row.try_get::<Option<f64>, _>(i)
                                .map(|v| v.map_or("NULL".to_string(), |n| n.to_string()))
                        })
                        .or_else(|_| {
                            row.try_get::<Option<bool>, _>(i)
                                .map(|v| v.map_or("NULL".to_string(), |b| b.to_string()))
                        })
                        .unwrap_or_else(|_| "<?>".to_string())
                })
                .collect()
        })
        .collect();

    Ok(TableDataResult {
        table_name: table_name.to_string(),
        columns,
        rows: string_rows,
        total_count: total_count.0,
        page,
    })
}
