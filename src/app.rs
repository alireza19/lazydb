use crate::event::{AppEvent, Event, EventHandler, QueryResult, TableDataResult};
use clap::Parser;
use ratatui::{
    crossterm::event::{KeyCode, KeyEvent, KeyModifiers},
    DefaultTerminal,
};
use sqlx::{Column, PgPool, Row};
use std::collections::VecDeque;
use std::env;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tracing::{debug, info};
use tui_textarea::TextArea;

/// Page size for table data pagination.
pub const PAGE_SIZE: usize = 50;

/// Maximum query history size.
pub const MAX_HISTORY: usize = 20;

/// Default visible rows for scroll calculations.
/// This is a conservative estimate; actual visible rows depend on terminal size.
pub const DEFAULT_VISIBLE_ROWS: usize = 15;

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
    /// Scroll offset for the visible window.
    pub scroll_offset: usize,
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

    /// Update scroll offset to keep selected row visible.
    pub fn ensure_visible(&mut self, visible_rows: usize) {
        if visible_rows == 0 {
            return;
        }
        if self.selected_row < self.scroll_offset {
            self.scroll_offset = self.selected_row;
        }
        if self.selected_row >= self.scroll_offset + visible_rows {
            self.scroll_offset = self.selected_row.saturating_sub(visible_rows - 1);
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

/// Which pane has focus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FocusedPane {
    /// Sidebar/results navigation.
    Navigation,
    /// SQL editor.
    SqlEditor,
}

/// SQL query result state for display.
#[derive(Debug, Clone)]
pub struct QueryResultState {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub row_count: usize,
    pub duration_ms: u128,
    pub is_explain: bool,
    pub selected_row: usize,
    pub scroll_offset: usize,
    pub error: Option<String>,
}

impl QueryResultState {
    /// Update scroll offset to keep selected row visible.
    pub fn ensure_visible(&mut self, visible_rows: usize) {
        if visible_rows == 0 {
            return;
        }
        // Scroll up if selected is above viewport
        if self.selected_row < self.scroll_offset {
            self.scroll_offset = self.selected_row;
        }
        // Scroll down if selected is below viewport
        if self.selected_row >= self.scroll_offset + visible_rows {
            self.scroll_offset = self.selected_row.saturating_sub(visible_rows - 1);
        }
    }
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
    /// Which pane is focused.
    pub focused_pane: FocusedPane,
    /// SQL editor text area.
    pub sql_editor: TextArea<'static>,
    /// Query history.
    pub query_history: VecDeque<String>,
    /// Current position in history (None = not browsing history).
    pub history_index: Option<usize>,
    /// Saved editor content when browsing history.
    pub saved_editor_content: Option<String>,
    /// Is a query currently executing?
    pub query_executing: bool,
    /// Query start time for duration display.
    pub query_start_time: Option<Instant>,
    /// Last query result.
    pub query_result: Option<QueryResultState>,
    /// Show query results instead of table view.
    pub show_query_results: bool,
}

impl std::fmt::Debug for App {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("App")
            .field("running", &self.running)
            .field("connection", &self.connection)
            .field("current_view", &self.current_view)
            .field("tables", &self.tables)
            .field("selected_table_index", &self.selected_table_index)
            .field("focused_pane", &self.focused_pane)
            .field("query_executing", &self.query_executing)
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

        let mut sql_editor = TextArea::default();
        sql_editor.set_cursor_line_style(ratatui::style::Style::default());
        sql_editor.set_placeholder_text("-- type : to focus · Ctrl+Enter to run");

        Self {
            running: true,
            connection: ConnectionState::Connecting,
            current_view: CurrentView::ConnectionStatus,
            tables: Vec::new(),
            selected_table_index: 0,
            events,
            refresh_handle: None,
            focused_pane: FocusedPane::Navigation,
            sql_editor,
            query_history: VecDeque::new(),
            history_index: None,
            saved_editor_content: None,
            query_executing: false,
            query_start_time: None,
            query_result: None,
            show_query_results: false,
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
                if self.tables != new_tables {
                    let previously_selected = self
                        .tables
                        .get(self.selected_table_index)
                        .cloned();

                    self.tables = new_tables;

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

                if self.refresh_handle.is_none() {
                    self.start_refresh_task();
                }
            }
            AppEvent::TableDataLoaded(result) => {
                match result {
                    Ok(data) => {
                        if let CurrentView::TableView(ref mut state) = self.current_view
                            && state.table_name == data.table_name
                            && state.page == data.page
                        {
                            state.columns = data.columns;
                            state.rows = data.rows;
                            state.total_count = data.total_count;
                            state.loading = false;
                            state.error = None;
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
            AppEvent::QueryExecuted(result) => {
                self.query_executing = false;
                self.query_start_time = None;

                match result {
                    Ok(qr) => {
                        self.query_result = Some(QueryResultState {
                            columns: qr.columns,
                            rows: qr.rows,
                            row_count: qr.row_count,
                            duration_ms: qr.duration_ms,
                            is_explain: qr.is_explain,
                            selected_row: 0,
                            scroll_offset: 0,
                            error: None,
                        });
                        self.show_query_results = true;
                    }
                    Err(error) => {
                        self.query_result = Some(QueryResultState {
                            columns: Vec::new(),
                            rows: Vec::new(),
                            row_count: 0,
                            duration_ms: 0,
                            is_explain: false,
                            selected_row: 0,
                            scroll_offset: 0,
                            error: Some(error),
                        });
                        self.show_query_results = true;
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

    /// Execute SQL query.
    fn execute_query(&mut self) {
        let query = self.sql_editor.lines().join("\n").trim().to_string();
        if query.is_empty() {
            return;
        }

        // Add to history
        if self.query_history.front() != Some(&query) {
            self.query_history.push_front(query.clone());
            if self.query_history.len() > MAX_HISTORY {
                self.query_history.pop_back();
            }
        }
        self.history_index = None;
        self.saved_editor_content = None;

        if let ConnectionState::Connected { pool, .. } = &self.connection {
            let pool = pool.clone();
            let sender = self.events.sender();

            self.query_executing = true;
            self.query_start_time = Some(Instant::now());

            info!("Executing query: {}", query);

            tokio::spawn(async move {
                let result = execute_sql_query(&pool, &query).await;
                let _ = sender.send(Event::App(AppEvent::QueryExecuted(result)));
            });
        }
    }

    /// Open a table for viewing.
    fn open_table(&mut self, table_name: &str) {
        info!("Opening table: {}", table_name);
        self.show_query_results = false;

        let state = TableViewState {
            table_name: table_name.to_string(),
            columns: Vec::new(),
            rows: Vec::new(),
            total_count: 0,
            page: 0,
            selected_row: 0,
            scroll_offset: 0,
            loading: true,
            error: None,
        };

        self.current_view = CurrentView::TableView(state);
        self.fetch_table_data(table_name, 0);
    }

    /// Handles the key events and updates the state of [`App`].
    pub fn handle_key_events(&mut self, key_event: KeyEvent) -> color_eyre::Result<()> {
        // Global quit with Ctrl+C
        if matches!(key_event.code, KeyCode::Char('c') | KeyCode::Char('C'))
            && key_event.modifiers == KeyModifiers::CONTROL
        {
            self.events.send(AppEvent::Quit);
            return Ok(());
        }

        // Handle based on focused pane
        match self.focused_pane {
            FocusedPane::SqlEditor => self.handle_editor_keys(key_event),
            FocusedPane::Navigation => self.handle_navigation_keys(key_event),
        }
    }

    /// Handle keys when SQL editor is focused.
    fn handle_editor_keys(&mut self, key_event: KeyEvent) -> color_eyre::Result<()> {
        // Debug: log key events to help diagnose
        debug!(
            "Editor key: code={:?} modifiers={:?}",
            key_event.code, key_event.modifiers
        );

        // Check if this is an "execute query" key combination
        let is_execute_key = self.is_execute_key_combo(&key_event);

        if is_execute_key {
            debug!("Execute key combo detected!");
            if !self.query_executing {
                self.execute_query();
            }
            return Ok(());
        }

        // Escape to go back to navigation
        if key_event.code == KeyCode::Esc {
            self.focused_pane = FocusedPane::Navigation;
            return Ok(());
        }

        // History navigation with Up/Down when editor is empty or at boundaries
        if key_event.code == KeyCode::Up && key_event.modifiers.is_empty() {
            let (row, _) = self.sql_editor.cursor();
            if row == 0 && !self.query_history.is_empty() {
                self.navigate_history_up();
                return Ok(());
            }
        }

        if key_event.code == KeyCode::Down && key_event.modifiers.is_empty() {
            let (row, _) = self.sql_editor.cursor();
            let line_count = self.sql_editor.lines().len();
            if row >= line_count.saturating_sub(1) && self.history_index.is_some() {
                self.navigate_history_down();
                return Ok(());
            }
        }

        // Pass other keys to the text area
        self.sql_editor.input(key_event);
        Ok(())
    }

    /// Check if the key event is an "execute query" combination.
    /// Supports multiple key combos for cross-platform compatibility:
    /// - Ctrl+Enter (standard)
    /// - Cmd+Enter (macOS)
    /// - Ctrl+J (Enter is often Ctrl+J / \n)
    /// - F5 (common in SQL tools)
    fn is_execute_key_combo(&self, key_event: &KeyEvent) -> bool {
        let ctrl = key_event.modifiers.contains(KeyModifiers::CONTROL);
        let cmd = key_event.modifiers.contains(KeyModifiers::SUPER);
        let shift = key_event.modifiers.contains(KeyModifiers::SHIFT);

        match key_event.code {
            // Ctrl+Enter or Cmd+Enter
            KeyCode::Enter if ctrl || cmd => true,
            // Ctrl+J (Enter equivalent on Unix - \n is Ctrl+J)
            KeyCode::Char('j') | KeyCode::Char('J') if ctrl => true,
            // Shift+Enter as alternative
            KeyCode::Enter if shift => true,
            // F5 (common SQL execute key)
            KeyCode::F(5) => true,
            _ => false,
        }
    }

    /// Navigate up in query history.
    fn navigate_history_up(&mut self) {
        if self.query_history.is_empty() {
            return;
        }

        // Save current content if just starting to browse
        if self.history_index.is_none() {
            self.saved_editor_content = Some(self.sql_editor.lines().join("\n"));
        }

        let new_index = match self.history_index {
            None => 0,
            Some(i) => (i + 1).min(self.query_history.len() - 1),
        };

        self.history_index = Some(new_index);

        if let Some(query) = self.query_history.get(new_index) {
            self.sql_editor = TextArea::new(query.lines().map(String::from).collect());
            self.sql_editor.set_cursor_line_style(ratatui::style::Style::default());
        }
    }

    /// Navigate down in query history.
    fn navigate_history_down(&mut self) {
        match self.history_index {
            None => {}
            Some(0) => {
                // Return to saved content
                self.history_index = None;
                if let Some(content) = self.saved_editor_content.take() {
                    self.sql_editor = TextArea::new(content.lines().map(String::from).collect());
                    self.sql_editor.set_cursor_line_style(ratatui::style::Style::default());
                }
            }
            Some(i) => {
                let new_index = i - 1;
                self.history_index = Some(new_index);
                if let Some(query) = self.query_history.get(new_index) {
                    self.sql_editor = TextArea::new(query.lines().map(String::from).collect());
                    self.sql_editor.set_cursor_line_style(ratatui::style::Style::default());
                }
            }
        }
    }

    /// Handle keys when navigation pane is focused.
    fn handle_navigation_keys(&mut self, key_event: KeyEvent) -> color_eyre::Result<()> {
        // ':' to focus SQL editor
        if key_event.code == KeyCode::Char(':') {
            self.focused_pane = FocusedPane::SqlEditor;
            return Ok(());
        }

        // Clear query results with 'c'
        if key_event.code == KeyCode::Char('c') && self.show_query_results {
            self.show_query_results = false;
            self.query_result = None;
            return Ok(());
        }

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
                        if self.show_query_results {
                            if let Some(ref mut qr) = self.query_result
                                && !qr.rows.is_empty()
                            {
                                if qr.selected_row > 0 {
                                    qr.selected_row -= 1;
                                } else {
                                    qr.selected_row = qr.rows.len() - 1;
                                    // Jump to end - reset scroll to show selection
                                    qr.scroll_offset = qr.rows.len().saturating_sub(DEFAULT_VISIBLE_ROWS);
                                }
                                qr.ensure_visible(DEFAULT_VISIBLE_ROWS);
                            }
                        } else if !self.tables.is_empty() {
                            if self.selected_table_index > 0 {
                                self.selected_table_index -= 1;
                            } else {
                                self.selected_table_index = self.tables.len() - 1;
                            }
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if self.show_query_results {
                            if let Some(ref mut qr) = self.query_result
                                && !qr.rows.is_empty()
                            {
                                if qr.selected_row < qr.rows.len() - 1 {
                                    qr.selected_row += 1;
                                } else {
                                    qr.selected_row = 0;
                                    qr.scroll_offset = 0; // Jump to beginning
                                }
                                qr.ensure_visible(DEFAULT_VISIBLE_ROWS);
                            }
                        } else if !self.tables.is_empty() {
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
                        self.current_view = CurrentView::TableList;
                        self.show_query_results = false;
                    }
                    KeyCode::Char('q') => self.events.send(AppEvent::Quit),
                    KeyCode::Up | KeyCode::Char('k') => {
                        if self.show_query_results {
                            if let Some(ref mut qr) = self.query_result
                                && !qr.rows.is_empty()
                            {
                                if qr.selected_row > 0 {
                                    qr.selected_row -= 1;
                                } else {
                                    qr.selected_row = qr.rows.len() - 1;
                                    qr.scroll_offset = qr.rows.len().saturating_sub(DEFAULT_VISIBLE_ROWS);
                                }
                                qr.ensure_visible(DEFAULT_VISIBLE_ROWS);
                            }
                        } else if !state.rows.is_empty() {
                            if state.selected_row > 0 {
                                state.selected_row -= 1;
                            } else {
                                state.selected_row = state.rows.len() - 1;
                                state.scroll_offset = state.rows.len().saturating_sub(DEFAULT_VISIBLE_ROWS);
                            }
                            state.ensure_visible(DEFAULT_VISIBLE_ROWS);
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if self.show_query_results {
                            if let Some(ref mut qr) = self.query_result
                                && !qr.rows.is_empty()
                            {
                                if qr.selected_row < qr.rows.len() - 1 {
                                    qr.selected_row += 1;
                                } else {
                                    qr.selected_row = 0;
                                    qr.scroll_offset = 0;
                                }
                                qr.ensure_visible(DEFAULT_VISIBLE_ROWS);
                            }
                        } else if !state.rows.is_empty() {
                            if state.selected_row < state.rows.len() - 1 {
                                state.selected_row += 1;
                            } else {
                                state.selected_row = 0;
                                state.scroll_offset = 0;
                            }
                            state.ensure_visible(DEFAULT_VISIBLE_ROWS);
                        }
                    }
                    KeyCode::Left | KeyCode::Char('h') => {
                        if !self.show_query_results && state.page > 0 && !state.loading {
                            state.page -= 1;
                            state.loading = true;
                            state.selected_row = 0;
                            state.scroll_offset = 0;
                            fetch_page = Some((state.table_name.clone(), state.page));
                        }
                    }
                    KeyCode::Right | KeyCode::Char('l') => {
                        if !self.show_query_results {
                            let total_pages = state.total_pages();
                            if state.page < total_pages.saturating_sub(1) && !state.loading {
                                state.page += 1;
                                state.loading = true;
                                state.selected_row = 0;
                                state.scroll_offset = 0;
                                fetch_page = Some((state.table_name.clone(), state.page));
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

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

    /// Get elapsed query time in ms.
    pub fn query_elapsed_ms(&self) -> Option<u128> {
        self.query_start_time.map(|t| t.elapsed().as_millis())
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

    let count_query = format!(r#"SELECT COUNT(*) FROM "{}""#, table_name);
    let total_count: (i64,) = sqlx::query_as(&count_query)
        .fetch_one(pool)
        .await
        .map_err(|e| format!("Failed to get row count: {e}"))?;

    let data_query = format!(
        r#"SELECT * FROM "{}" LIMIT {} OFFSET {}"#,
        table_name, PAGE_SIZE, offset
    );

    let rows = sqlx::query(&data_query)
        .fetch_all(pool)
        .await
        .map_err(|e| format!("Failed to fetch data: {e}"))?;

    let columns: Vec<String> = if rows.is_empty() {
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

    let string_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row_to_strings(row, columns.len()))
        .collect();

    Ok(TableDataResult {
        table_name: table_name.to_string(),
        columns,
        rows: string_rows,
        total_count: total_count.0,
        page,
    })
}

/// Execute a SQL query and return results.
async fn execute_sql_query(pool: &PgPool, query: &str) -> Result<QueryResult, String> {
    let start = Instant::now();
    let is_explain = query.trim().to_uppercase().starts_with("EXPLAIN");

    let rows = sqlx::query(query)
        .fetch_all(pool)
        .await
        .map_err(|e| format!("{e}"))?;

    let duration_ms = start.elapsed().as_millis();

    let columns: Vec<String> = if rows.is_empty() {
        Vec::new()
    } else {
        rows[0]
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect()
    };

    let string_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row_to_strings(row, columns.len()))
        .collect();

    let row_count = string_rows.len();

    Ok(QueryResult {
        query: query.to_string(),
        columns,
        rows: string_rows,
        row_count,
        duration_ms,
        is_explain,
    })
}

/// Convert a database row to a vector of strings.
fn row_to_strings(row: &sqlx::postgres::PgRow, col_count: usize) -> Vec<String> {
    (0..col_count)
        .map(|i| {
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
}
