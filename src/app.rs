use crate::event::{
    AppEvent, DatabaseStructure, DbColumn, DbSchema, DbTable, Event, EventHandler, QueryResult,
    StatsUpdate, TableDataResult,
};
use chrono::{DateTime, Utc};
use clap::Parser;
use ratatui::{
    DefaultTerminal,
    crossterm::event::{KeyCode, KeyEvent, KeyModifiers},
};
use sqlx::{AnyPool, Column, Row};
use std::cell::Cell;
use std::collections::VecDeque;
use std::env;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tracing::{debug, info};
use tui_logger::TuiWidgetState;
use tui_textarea::TextArea;
use tui_tree_widget::TreeState;

pub const PAGE_SIZE: usize = 50;
pub const MAX_HISTORY: usize = 20;
pub const DEFAULT_VISIBLE_ROWS: usize = 15;
pub const SCHEMA_REFRESH_SECS: u64 = 10;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TreeNodeId {
    Root,
    Schema(String),
    Table {
        schema: String,
        table: String,
    },
    Column {
        schema: String,
        table: String,
        column: String,
    },
}

#[derive(Parser, Debug)]
#[command(name = "lazydb")]
#[command(about = "A lazydocker-inspired database TUI written in Rust")]
#[command(
    after_help = "EXAMPLES:\n    lazydb postgres://user:pass@localhost/mydb\n    lazydb  # uses DATABASE_URL or config file"
)]
pub struct Cli {
    /// Database connection URL (postgres://user:pass@host/db)
    #[arg(value_name = "URL")]
    pub url: Option<String>,

    /// Database URL (deprecated, use positional argument instead)
    #[arg(long = "url", short = 'u', hide = true)]
    pub url_flag: Option<String>,
}

/// Config file structure for ~/.config/lazydb/config.toml
#[derive(Debug, Default, serde::Deserialize)]
pub struct Config {
    #[serde(default)]
    pub connection: ConnectionConfig,
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct ConnectionConfig {
    pub default: Option<String>,
}

/// Saved connection entry for connections.toml
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SavedConnection {
    pub name: String,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used: Option<DateTime<Utc>>,
}

impl SavedConnection {
    pub fn new(name: String, url: String) -> Self {
        Self {
            name,
            url,
            last_used: None,
        }
    }

    /// Extract host/database info from URL for display
    pub fn display_host(&self) -> String {
        self.url
            .find('@')
            .map(|at| {
                let after = &self.url[at + 1..];
                after
                    .find('/')
                    .map_or(after.to_string(), |slash| after[..slash].to_string())
            })
            .unwrap_or_else(|| "localhost".to_string())
    }

    /// Detect database type from URL
    pub fn db_type(&self) -> &'static str {
        if self.url.starts_with("postgres://") || self.url.starts_with("postgresql://") {
            "postgres"
        } else if self.url.starts_with("mysql://") {
            "mysql"
        } else if self.url.starts_with("sqlite://") {
            "sqlite"
        } else {
            "unknown"
        }
    }
}

/// File structure for ~/.config/lazydb/connections.toml
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ConnectionsFile {
    #[serde(default)]
    pub connections: Vec<SavedConnection>,
}

impl ConnectionsFile {
    pub fn path() -> std::path::PathBuf {
        let config_dir = env::var("XDG_CONFIG_HOME")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                directories::BaseDirs::new()
                    .map(|dirs| dirs.home_dir().join(".config"))
                    .unwrap_or_else(|| std::path::PathBuf::from("~/.config"))
            });
        config_dir.join("lazydb").join("connections.toml")
    }

    pub fn load() -> Self {
        let path = Self::path();
        if !path.exists() {
            let default = Self::create_default();
            let _ = default.save();
            return default;
        }

        std::fs::read_to_string(&path)
            .ok()
            .and_then(|contents| toml::from_str(&contents).ok())
            .unwrap_or_else(Self::create_default)
    }

    pub fn save(&self) -> color_eyre::Result<()> {
        let path = Self::path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let contents = toml::to_string_pretty(self)?;
        std::fs::write(&path, contents)?;
        Ok(())
    }

    fn create_default() -> Self {
        // Start with empty connections - user will add their own
        Self {
            connections: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    Csv,
    Tsv,
    Json,
    Markdown,
}

impl ExportFormat {
    pub fn extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Tsv => "tsv",
            Self::Json => "json",
            Self::Markdown => "md",
        }
    }
}

/// State for the connection manager modal
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionManagerMode {
    List,
    AddingName,
    AddingUrl,
}

#[derive(Debug)]
pub struct ConnectionManagerState {
    pub visible: bool,
    pub connections: Vec<SavedConnection>,
    pub selected_index: usize,
    pub scroll_x: usize,
    pub mode: ConnectionManagerMode,
    pub input_name: String,
    pub input_url: String,
}

impl Default for ConnectionManagerState {
    fn default() -> Self {
        Self {
            visible: false,
            connections: Vec::new(),
            selected_index: 0,
            scroll_x: 0,
            mode: ConnectionManagerMode::List,
            input_name: String::new(),
            input_url: String::new(),
        }
    }
}

impl ConnectionManagerState {
    pub fn open(&mut self) {
        let file = ConnectionsFile::load();
        self.connections = file.connections;
        self.visible = true;
        self.selected_index = 0;
        self.scroll_x = 0;
        self.mode = ConnectionManagerMode::List;
        self.input_name.clear();
        self.input_url.clear();
    }

    pub fn close(&mut self) {
        self.visible = false;
        self.scroll_x = 0;
        self.mode = ConnectionManagerMode::List;
        self.input_name.clear();
        self.input_url.clear();
    }

    pub fn save_connections(&self) {
        let file = ConnectionsFile {
            connections: self.connections.clone(),
        };
        let _ = file.save();
    }

    pub fn start_adding(&mut self) {
        self.mode = ConnectionManagerMode::AddingName;
        self.input_name.clear();
        self.input_url.clear();
    }

    pub fn finish_adding(&mut self) -> bool {
        if self.input_name.trim().is_empty() || self.input_url.trim().is_empty() {
            return false;
        }
        self.connections.push(SavedConnection::new(
            self.input_name.trim().to_string(),
            self.input_url.trim().to_string(),
        ));
        self.selected_index = self.connections.len().saturating_sub(1);
        self.save_connections();
        self.mode = ConnectionManagerMode::List;
        self.input_name.clear();
        self.input_url.clear();
        true
    }

    pub fn cancel_adding(&mut self) {
        self.mode = ConnectionManagerMode::List;
        self.input_name.clear();
        self.input_url.clear();
    }

    pub fn delete_selected(&mut self) {
        if !self.connections.is_empty() {
            self.connections.remove(self.selected_index);
            if self.selected_index >= self.connections.len() && !self.connections.is_empty() {
                self.selected_index = self.connections.len() - 1;
            }
            self.save_connections();
        }
    }

    pub fn mark_used(&mut self, index: usize) {
        if let Some(conn) = self.connections.get_mut(index) {
            conn.last_used = Some(Utc::now());
            self.save_connections();
        }
    }

    pub fn navigate(&mut self, delta: i32) {
        if self.connections.is_empty() {
            return;
        }
        let len = self.connections.len() as i32;
        let new_idx = (self.selected_index as i32 + delta).rem_euclid(len);
        self.selected_index = new_idx as usize;
    }
}

impl Cli {
    /// Try to get database URL from available sources.
    /// Returns None if no URL is configured (graceful fallback to connection manager).
    pub fn get_database_url(&self) -> Option<String> {
        // Priority: positional arg → --url flag → DATABASE_URL env → config file
        if let Some(url) = &self.url {
            return Some(url.clone());
        }

        if let Some(url) = &self.url_flag {
            return Some(url.clone());
        }

        if let Ok(url) = env::var("DATABASE_URL") {
            return Some(url);
        }

        if let Some(url) = Self::load_url_from_config().ok().flatten() {
            return Some(url);
        }

        // Ensure config exists for future use, but don't fail
        let _ = Self::ensure_config_exists();

        None
    }

    fn config_path() -> std::path::PathBuf {
        // Use XDG_CONFIG_HOME if set, otherwise ~/.config (consistent across platforms)
        let config_dir = env::var("XDG_CONFIG_HOME")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                directories::BaseDirs::new()
                    .map(|dirs| dirs.home_dir().join(".config"))
                    .unwrap_or_else(|| std::path::PathBuf::from("~/.config"))
            });
        config_dir.join("lazydb").join("config.toml")
    }

    fn load_url_from_config() -> color_eyre::Result<Option<String>> {
        let config_path = Self::config_path();
        if !config_path.exists() {
            return Ok(None);
        }

        let contents = std::fs::read_to_string(&config_path).map_err(|e| {
            color_eyre::eyre::eyre!(
                "Failed to read config file {}: {}",
                config_path.display(),
                e
            )
        })?;

        let config: Config = toml::from_str(&contents).map_err(|e| {
            color_eyre::eyre::eyre!(
                "Failed to parse config file {}: {}",
                config_path.display(),
                e
            )
        })?;

        Ok(config.connection.default)
    }

    fn ensure_config_exists() -> color_eyre::Result<()> {
        let config_path = Self::config_path();
        if config_path.exists() {
            return Ok(());
        }

        if let Some(parent) = config_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }

        let default_config = r#"# lazydb configuration
# Uncomment and edit the line below to set your default database connection

[connection]
# default = "postgres://user:password@localhost:5432/database"
"#;

        std::fs::write(&config_path, default_config).map_err(|e| {
            color_eyre::eyre::eyre!(
                "Failed to create config file {}: {}",
                config_path.display(),
                e
            )
        })?;

        tracing::info!("Created config file at {}", config_path.display());
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbType {
    Postgres,
    MySQL,
}

impl DbType {
    pub fn from_url(url: &str) -> Self {
        if url.starts_with("mysql://") || url.starts_with("mariadb://") {
            Self::MySQL
        } else {
            Self::Postgres
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Postgres => "PostgreSQL",
            Self::MySQL => "MySQL",
        }
    }
}

#[derive(Debug)]
pub enum ConnectionState {
    NotConfigured,
    Connecting,
    Connected {
        pool: AnyPool,
        db_name: String,
        db_type: DbType,
    },
    Failed {
        error: String,
    },
}

#[derive(Debug, Clone)]
pub struct TableViewState {
    pub table_name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub total_count: i64,
    pub page: usize,
    pub selected_row: usize,
    pub scroll_offset: usize,
    pub col_offset: usize,
    pub loading: bool,
    pub error: Option<String>,
}

impl TableViewState {
    pub fn total_pages(&self) -> usize {
        if self.total_count == 0 {
            1
        } else {
            (self.total_count as usize).div_ceil(PAGE_SIZE)
        }
    }

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

#[derive(Debug, Clone)]
pub enum CurrentView {
    ConnectionStatus,
    TableList,
    TableView(TableViewState),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FocusedPane {
    Sidebar,
    Stats,
    Logs,
    Results,
    Editor,
}

impl FocusedPane {
    /// Tab order: Sidebar → Results → Editor → Logs → Stats → (loop)
    pub fn next(self) -> Self {
        match self {
            Self::Sidebar => Self::Results,
            Self::Results => Self::Editor,
            Self::Editor => Self::Logs,
            Self::Logs => Self::Stats,
            Self::Stats => Self::Sidebar,
        }
    }

    pub fn prev(self) -> Self {
        match self {
            Self::Sidebar => Self::Stats,
            Self::Results => Self::Sidebar,
            Self::Editor => Self::Results,
            Self::Logs => Self::Editor,
            Self::Stats => Self::Logs,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Sidebar => "Tables",
            Self::Stats => "Stats",
            Self::Logs => "Logs",
            Self::Results => "Results",
            Self::Editor => "SQL",
        }
    }
}

pub const SPARKLINE_MAX_POINTS: usize = 60;

#[derive(Debug, Clone)]
pub struct StatsState {
    pub host: String,
    pub database: String,
    pub pg_version: String,
    pub table_count: usize,
    pub total_rows: i64,
    pub last_query_ms: Option<u128>,
    pub queries_run: usize,
    pub session_start: Instant,
    pub queries_per_sec: VecDeque<u64>,
    pub rows_per_sec: VecDeque<u64>,
    pub latency_ms: VecDeque<u64>,
    pub connections: VecDeque<u64>,
    pub queries_this_second: u64,
    pub rows_this_second: u64,
}

impl StatsState {
    fn push_sparkline(deque: &mut VecDeque<u64>, value: u64) {
        if deque.len() >= SPARKLINE_MAX_POINTS {
            deque.pop_front();
        }
        deque.push_back(value);
    }

    pub fn record_query(&mut self, duration_ms: u128, row_count: usize) {
        self.queries_run += 1;
        self.last_query_ms = Some(duration_ms);
        self.queries_this_second += 1;
        self.rows_this_second += row_count as u64;
    }

    pub fn tick_second(&mut self, pool_size: u32) {
        Self::push_sparkline(&mut self.queries_per_sec, self.queries_this_second);
        Self::push_sparkline(&mut self.rows_per_sec, self.rows_this_second);
        Self::push_sparkline(&mut self.latency_ms, self.last_query_ms.unwrap_or(0) as u64);
        Self::push_sparkline(&mut self.connections, pool_size as u64);
        self.queries_this_second = 0;
        self.rows_this_second = 0;
    }
}

#[derive(Debug, Clone)]
pub struct QueryResultState {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub row_count: usize,
    pub duration_ms: u128,
    pub is_explain: bool,
    pub selected_row: usize,
    pub scroll_offset: usize,
    pub col_offset: usize,
    pub error: Option<String>,
}

impl QueryResultState {
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

pub struct App {
    pub running: bool,
    pub connection: ConnectionState,
    pub database_url: String,
    pub current_view: CurrentView,
    pub tables: Vec<String>,
    pub selected_table_index: usize,
    pub sidebar_scroll_offset: usize,
    pub events: EventHandler,
    stats_handle: Option<JoinHandle<()>>,
    schema_handle: Option<JoinHandle<()>>,
    pub focused_pane: FocusedPane,
    pub sql_editor: TextArea<'static>,
    pub editor_scroll_offset: usize,
    pub query_history: VecDeque<String>,
    pub history_index: Option<usize>,
    pub saved_editor_content: Option<String>,
    pub query_executing: bool,
    pub query_start_time: Option<Instant>,
    pub query_result: Option<QueryResultState>,
    pub show_query_results: bool,
    pub stats: StatsState,
    pub stats_scroll_offset: usize,
    pub logs_state: TuiWidgetState,
    pub db_structure: Option<DatabaseStructure>,
    pub tree_state: TreeState<TreeNodeId>,
    pub selected_table: Option<(String, String)>,
    pub connection_manager: ConnectionManagerState,
    pub export_modal: bool,
    pub export_message: Option<(String, Instant)>,
    /// Actual data-table area height set during render (interior mutability).
    pub results_area_height: Cell<u16>,
    /// Actual SQL editor area height set during render (interior mutability).
    pub editor_area_height: Cell<u16>,
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
            .field("stats", &self.stats)
            .field("db_structure", &self.db_structure)
            .field("selected_table", &self.selected_table)
            .field(
                "connection_manager.visible",
                &self.connection_manager.visible,
            )
            .finish()
    }
}

fn parse_host_from_url(url: &str) -> String {
    url.find('@')
        .map(|at| {
            let after = &url[at + 1..];
            after
                .find('/')
                .map_or(after, |slash| &after[..slash])
                .to_string()
        })
        .unwrap_or_else(|| "localhost".to_string())
}

impl App {
    pub fn new(database_url: Option<String>) -> Self {
        let events = EventHandler::new();

        let mut sql_editor = TextArea::default();
        sql_editor.set_cursor_line_style(ratatui::style::Style::default());
        sql_editor.set_placeholder_text("-- type : to focus · F5 to run");

        // If we have a URL, start connecting; otherwise open connection manager
        let (connection_state, host, url, connection_manager) = match database_url {
            Some(url) => {
                let sender = events.sender();
                let host = parse_host_from_url(&url);
                let url_for_task = url.clone();

                tokio::spawn(async move {
                    let result = connect_to_database(&url_for_task).await;
                    let _ = sender.send(Event::App(AppEvent::ConnectionResult(result)));
                });

                (
                    ConnectionState::Connecting,
                    host,
                    url,
                    ConnectionManagerState::default(),
                )
            }
            None => {
                // No URL provided - open connection manager immediately
                let mut cm = ConnectionManagerState::default();
                cm.open();
                (
                    ConnectionState::NotConfigured,
                    String::new(),
                    String::new(),
                    cm,
                )
            }
        };

        Self {
            running: true,
            connection: connection_state,
            database_url: url,
            current_view: CurrentView::ConnectionStatus,
            tables: Vec::new(),
            selected_table_index: 0,
            sidebar_scroll_offset: 0,
            events,
            stats_handle: None,
            schema_handle: None,
            focused_pane: FocusedPane::Sidebar,
            sql_editor,
            editor_scroll_offset: 0,
            query_history: VecDeque::new(),
            history_index: None,
            saved_editor_content: None,
            query_executing: false,
            query_start_time: None,
            query_result: None,
            show_query_results: false,
            stats: StatsState {
                host,
                database: String::new(),
                pg_version: String::new(),
                table_count: 0,
                total_rows: 0,
                last_query_ms: None,
                queries_run: 0,
                session_start: Instant::now(),
                queries_per_sec: VecDeque::with_capacity(SPARKLINE_MAX_POINTS),
                rows_per_sec: VecDeque::with_capacity(SPARKLINE_MAX_POINTS),
                latency_ms: VecDeque::with_capacity(SPARKLINE_MAX_POINTS),
                connections: VecDeque::with_capacity(SPARKLINE_MAX_POINTS),
                queries_this_second: 0,
                rows_this_second: 0,
            },
            stats_scroll_offset: 0,
            logs_state: TuiWidgetState::default(),
            db_structure: None,
            tree_state: TreeState::default(),
            selected_table: None,
            connection_manager,
            export_modal: false,
            export_message: None,
            results_area_height: Cell::new(DEFAULT_VISIBLE_ROWS as u16),
            editor_area_height: Cell::new(DEFAULT_VISIBLE_ROWS as u16),
        }
    }

    pub async fn run(mut self, mut terminal: DefaultTerminal) -> color_eyre::Result<()> {
        while self.running {
            terminal.draw(|frame| frame.render_widget(&self, frame.area()))?;
            match self.events.next().await? {
                Event::Tick => {
                    if let Some((_, ts)) = &self.export_message {
                        if ts.elapsed() >= Duration::from_secs(2) {
                            self.export_message = None;
                        }
                    }
                }
                Event::Crossterm(event) => match event {
                    crossterm::event::Event::Key(key_event)
                        if key_event.kind == crossterm::event::KeyEventKind::Press =>
                    {
                        self.handle_key_events(key_event)?
                    }
                    crossterm::event::Event::Paste(data) => {
                        if self.connection_manager.visible {
                            // Handle paste in connection manager input fields
                            match self.connection_manager.mode {
                                ConnectionManagerMode::AddingName => {
                                    self.connection_manager.input_name.push_str(&data);
                                }
                                ConnectionManagerMode::AddingUrl => {
                                    self.connection_manager.input_url.push_str(&data);
                                }
                                ConnectionManagerMode::List => {}
                            }
                        } else if self.focused_pane == FocusedPane::Editor {
                            self.sql_editor.insert_str(&data);
                        }
                    }
                    crossterm::event::Event::Mouse(mouse_event) => {
                        self.handle_mouse_event(mouse_event);
                    }
                    _ => {}
                },
                Event::App(app_event) => self.handle_app_event(app_event),
            }
        }
        Ok(())
    }

    fn handle_mouse_event(&mut self, event: crossterm::event::MouseEvent) {
        use crossterm::event::MouseEventKind;
        match event.kind {
            MouseEventKind::ScrollUp => self.scroll_focused_pane(-3),
            MouseEventKind::ScrollDown => self.scroll_focused_pane(3),
            _ => {}
        }
    }

    fn scroll_focused_pane(&mut self, delta: i32) {
        match self.focused_pane {
            FocusedPane::Sidebar => self.tree_navigate(delta),
            FocusedPane::Results => self.scroll_results(delta),
            FocusedPane::Editor => self.scroll_editor(delta),
            FocusedPane::Stats => {
                self.stats_scroll_offset = if delta < 0 {
                    self.stats_scroll_offset.saturating_sub((-delta) as usize)
                } else {
                    self.stats_scroll_offset + delta as usize
                };
            }
            FocusedPane::Logs => {
                use tui_logger::TuiWidgetEvent;
                let event = if delta < 0 {
                    TuiWidgetEvent::UpKey
                } else {
                    TuiWidgetEvent::DownKey
                };
                for _ in 0..delta.unsigned_abs() {
                    self.logs_state.transition(event);
                }
            }
        }
    }

    fn scroll_results(&mut self, delta: i32) {
        if self.show_query_results {
            if let Some(ref mut qr) = self.query_result
                && !qr.rows.is_empty()
            {
                qr.selected_row = if delta < 0 {
                    qr.selected_row.saturating_sub((-delta) as usize)
                } else {
                    (qr.selected_row + delta as usize).min(qr.rows.len() - 1)
                };
                qr.ensure_visible(DEFAULT_VISIBLE_ROWS);
            }
        } else if let CurrentView::TableView(ref mut state) = self.current_view
            && !state.rows.is_empty()
        {
            state.selected_row = if delta < 0 {
                state.selected_row.saturating_sub((-delta) as usize)
            } else {
                (state.selected_row + delta as usize).min(state.rows.len() - 1)
            };
            state.ensure_visible(DEFAULT_VISIBLE_ROWS);
        }
    }

    fn scroll_editor(&mut self, delta: i32) {
        if self.sql_editor.lines().is_empty() {
            return;
        }
        let movement = if delta < 0 {
            tui_textarea::CursorMove::Up
        } else {
            tui_textarea::CursorMove::Down
        };
        for _ in 0..delta.unsigned_abs() {
            self.sql_editor.move_cursor(movement);
        }
        self.update_editor_scroll();
    }

    fn update_editor_scroll(&mut self) {
        let (cursor_row, _) = self.sql_editor.cursor();
        let visible_rows = (self.editor_area_height.get() as usize).max(1);
        if cursor_row < self.editor_scroll_offset {
            self.editor_scroll_offset = cursor_row;
        }
        if cursor_row >= self.editor_scroll_offset + visible_rows {
            self.editor_scroll_offset = cursor_row.saturating_sub(visible_rows - 1);
        }
    }

    fn handle_app_event(&mut self, event: AppEvent) {
        match event {
            AppEvent::Quit => self.running = false,
            AppEvent::ConnectionResult(result) => match result {
                Ok((pool, db_name, db_type)) => {
                    let sender = self.events.sender();
                    let pool_clone = pool.clone();
                    tokio::spawn(async move {
                        let structure = fetch_database_structure(&pool_clone, db_type).await;
                        let _ = sender.send(Event::App(AppEvent::SchemaLoaded(structure)));
                    });
                    self.stats.database = db_name.clone();
                    self.start_stats_task(&pool, db_type);
                    self.connection = ConnectionState::Connected {
                        pool,
                        db_name,
                        db_type,
                    };
                    self.current_view = CurrentView::TableList;
                }
                Err(error) => {
                    self.connection = ConnectionState::Failed { error };
                    self.current_view = CurrentView::ConnectionStatus;
                }
            },
            AppEvent::TablesLoaded(new_tables) => {
                if self.tables != new_tables {
                    let previously_selected = self.tables.get(self.selected_table_index).cloned();
                    self.tables = new_tables;
                    self.selected_table_index = previously_selected
                        .and_then(|name| self.tables.iter().position(|t| t == &name))
                        .unwrap_or(0);
                }
            }
            AppEvent::SchemaLoaded(structure) => {
                let table_count: usize = structure.schemas.iter().map(|s| s.tables.len()).sum();
                self.stats.table_count = table_count;

                self.tables = structure
                    .schemas
                    .iter()
                    .find(|s| s.name == "public")
                    .map(|s| s.tables.iter().map(|t| t.name.clone()).collect())
                    .unwrap_or_default();

                self.db_structure = Some(structure);

                if self.tree_state.selected().is_empty() {
                    self.tree_state.select(vec![TreeNodeId::Root]);
                    self.tree_state.open(vec![TreeNodeId::Root]);
                    self.tree_state.open(vec![
                        TreeNodeId::Root,
                        TreeNodeId::Schema("public".to_string()),
                    ]);
                }

                if self.schema_handle.is_none() {
                    self.start_schema_refresh_task();
                }
            }
            AppEvent::TableDataLoaded(result) => {
                if let CurrentView::TableView(ref mut state) = self.current_view {
                    match result {
                        Ok(data)
                            if state.table_name == data.table_name && state.page == data.page =>
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
                        Err(error) => {
                            state.loading = false;
                            state.error = Some(error);
                        }
                        _ => {}
                    }
                }
            }
            AppEvent::QueryExecuted(result) => {
                self.query_executing = false;
                self.query_start_time = None;
                match result {
                    Ok(qr) => {
                        self.stats.record_query(qr.duration_ms, qr.row_count);
                        self.query_result = Some(QueryResultState {
                            columns: qr.columns,
                            rows: qr.rows,
                            row_count: qr.row_count,
                            duration_ms: qr.duration_ms,
                            is_explain: qr.is_explain,
                            selected_row: 0,
                            scroll_offset: 0,
                            col_offset: 0,
                            error: None,
                        });
                    }
                    Err(error) => {
                        self.stats.queries_run += 1;
                        self.query_result = Some(QueryResultState {
                            columns: Vec::new(),
                            rows: Vec::new(),
                            row_count: 0,
                            duration_ms: 0,
                            is_explain: false,
                            selected_row: 0,
                            scroll_offset: 0,
                            col_offset: 0,
                            error: Some(error),
                        });
                    }
                }
                self.show_query_results = true;
            }
            AppEvent::StatsUpdated(update) => {
                self.stats.pg_version = update.pg_version;
                self.stats.total_rows = update.total_rows;
                self.stats.table_count = self.tables.len();
            }
            AppEvent::SparklineTick { pool_size } => {
                self.stats.tick_second(pool_size);
            }
        }
    }

    fn start_schema_refresh_task(&mut self) {
        let ConnectionState::Connected { pool, db_type, .. } = &self.connection else {
            return;
        };
        let pool = pool.clone();
        let db_type = *db_type;
        let sender = self.events.sender();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(SCHEMA_REFRESH_SECS));
            interval.tick().await;
            loop {
                interval.tick().await;
                if sender.is_closed() || pool.is_closed() {
                    break;
                }
                let structure = fetch_database_structure(&pool, db_type).await;
                if sender
                    .send(Event::App(AppEvent::SchemaLoaded(structure)))
                    .is_err()
                {
                    break;
                }
            }
        });
        self.schema_handle = Some(handle);
    }

    fn start_stats_task(&mut self, pool: &AnyPool, db_type: DbType) {
        let pool = pool.clone();
        let sender = self.events.sender();

        let handle = tokio::spawn(async move {
            if let Some(update) = fetch_stats(&pool, db_type).await {
                let _ = sender.send(Event::App(AppEvent::StatsUpdated(update)));
            }

            let mut sparkline_interval = tokio::time::interval(Duration::from_secs(1));
            let mut stats_counter = 0u32;
            sparkline_interval.tick().await;

            loop {
                sparkline_interval.tick().await;
                if sender.is_closed() || pool.is_closed() {
                    break;
                }
                let pool_size = pool.size();
                if sender
                    .send(Event::App(AppEvent::SparklineTick { pool_size }))
                    .is_err()
                {
                    break;
                }
                stats_counter += 1;
                if stats_counter >= 5 {
                    stats_counter = 0;
                    if let Some(update) = fetch_stats(&pool, db_type).await
                        && sender
                            .send(Event::App(AppEvent::StatsUpdated(update)))
                            .is_err()
                    {
                        break;
                    }
                }
            }
        });
        self.stats_handle = Some(handle);
    }

    fn fetch_table_data(&self, table_name: &str, page: usize) {
        let ConnectionState::Connected { pool, db_type, .. } = &self.connection else {
            return;
        };
        let pool = pool.clone();
        let db_type = *db_type;
        let sender = self.events.sender();
        let table_name = table_name.to_string();

        tokio::spawn(async move {
            let result = fetch_table_page(&pool, &table_name, page, db_type).await;
            let _ = sender.send(Event::App(AppEvent::TableDataLoaded(result)));
        });
    }

    fn execute_query(&mut self) {
        let query = self.sql_editor.lines().join("\n").trim().to_string();
        if query.is_empty() {
            return;
        }

        if self.query_history.front() != Some(&query) {
            self.query_history.push_front(query.clone());
            if self.query_history.len() > MAX_HISTORY {
                self.query_history.pop_back();
            }
        }
        self.history_index = None;
        self.saved_editor_content = None;

        let ConnectionState::Connected { pool, .. } = &self.connection else {
            return;
        };
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

    pub fn handle_key_events(&mut self, key_event: KeyEvent) -> color_eyre::Result<()> {
        if matches!(key_event.code, KeyCode::Char('c') | KeyCode::Char('C'))
            && key_event.modifiers == KeyModifiers::CONTROL
        {
            self.running = false;
            return Ok(());
        }

        // Handle export modal when visible
        if self.export_modal {
            return self.handle_export_modal_keys(key_event);
        }

        // Handle connection manager modal when visible
        if self.connection_manager.visible {
            return self.handle_connection_manager_keys(key_event);
        }

        if key_event.code == KeyCode::Tab {
            self.focused_pane = if key_event.modifiers.contains(KeyModifiers::SHIFT) {
                self.focused_pane.prev()
            } else {
                self.focused_pane.next()
            };
            return Ok(());
        }

        if key_event.code == KeyCode::BackTab {
            self.focused_pane = self.focused_pane.prev();
            return Ok(());
        }

        if key_event.code == KeyCode::Char(':') && self.focused_pane != FocusedPane::Editor {
            self.focused_pane = FocusedPane::Editor;
            return Ok(());
        }

        // Open connection manager with 'c' when not in Editor mode
        if key_event.code == KeyCode::Char('c')
            && self.focused_pane != FocusedPane::Editor
            && !self.show_query_results
        {
            self.connection_manager.open();
            return Ok(());
        }

        match self.focused_pane {
            FocusedPane::Editor => self.handle_editor_keys(key_event),
            FocusedPane::Sidebar => self.handle_sidebar_keys(key_event),
            FocusedPane::Results => self.handle_results_keys(key_event),
            FocusedPane::Stats => {
                if key_event.code == KeyCode::Char('q') {
                    self.running = false;
                }
                Ok(())
            }
            FocusedPane::Logs => self.handle_logs_keys(key_event),
        }
    }

    fn handle_connection_manager_keys(&mut self, key_event: KeyEvent) -> color_eyre::Result<()> {
        match &self.connection_manager.mode {
            ConnectionManagerMode::List => match key_event.code {
                KeyCode::Esc | KeyCode::Char('q') => {
                    self.connection_manager.close();
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.connection_manager.navigate(-1);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.connection_manager.navigate(1);
                }
                KeyCode::Enter => {
                    self.connect_to_selected();
                }
                KeyCode::Char('a') => {
                    self.connection_manager.start_adding();
                }
                KeyCode::Char('d') => {
                    self.connection_manager.delete_selected();
                }
                KeyCode::Left | KeyCode::Char('h') => {
                    self.connection_manager.scroll_x =
                        self.connection_manager.scroll_x.saturating_sub(4);
                }
                KeyCode::Right | KeyCode::Char('l') => {
                    self.connection_manager.scroll_x =
                        self.connection_manager.scroll_x.saturating_add(4);
                }
                _ => {}
            },
            ConnectionManagerMode::AddingName => match key_event.code {
                KeyCode::Esc => {
                    self.connection_manager.cancel_adding();
                }
                KeyCode::Enter => {
                    self.connection_manager.mode = ConnectionManagerMode::AddingUrl;
                }
                KeyCode::Backspace => {
                    self.connection_manager.input_name.pop();
                }
                KeyCode::Char(c) => {
                    self.connection_manager.input_name.push(c);
                }
                _ => {}
            },
            ConnectionManagerMode::AddingUrl => match key_event.code {
                KeyCode::Esc => {
                    self.connection_manager.cancel_adding();
                }
                KeyCode::Enter => {
                    self.connection_manager.finish_adding();
                }
                KeyCode::Backspace => {
                    self.connection_manager.input_url.pop();
                }
                KeyCode::Char(c) => {
                    self.connection_manager.input_url.push(c);
                }
                _ => {}
            },
        }
        Ok(())
    }

    fn connect_to_selected(&mut self) {
        let index = self.connection_manager.selected_index;
        if let Some(conn) = self.connection_manager.connections.get(index).cloned() {
            info!("Connecting to: {} ({})", conn.name, conn.display_host());
            self.connection_manager.mark_used(index);
            self.connection_manager.close();

            // Cancel existing background tasks
            if let Some(h) = self.stats_handle.take() {
                h.abort();
            }
            if let Some(h) = self.schema_handle.take() {
                h.abort();
            }

            // Reset state for new connection
            self.connection = ConnectionState::Connecting;
            self.current_view = CurrentView::ConnectionStatus;
            self.database_url = conn.url.clone();
            self.db_structure = None;
            self.tables.clear();
            self.selected_table = None;
            self.stats.host = conn.display_host();
            self.stats.database.clear();

            // Start new connection
            let sender = self.events.sender();
            let url = conn.url;
            tokio::spawn(async move {
                let result = connect_to_database(&url).await;
                let _ = sender.send(Event::App(AppEvent::ConnectionResult(result)));
            });
        }
    }

    fn handle_logs_keys(&mut self, key_event: KeyEvent) -> color_eyre::Result<()> {
        use tui_logger::TuiWidgetEvent;

        let event = match key_event.code {
            KeyCode::Char('q') => {
                self.running = false;
                return Ok(());
            }
            KeyCode::Up | KeyCode::Char('k') => Some(TuiWidgetEvent::UpKey),
            KeyCode::Down | KeyCode::Char('j') => Some(TuiWidgetEvent::DownKey),
            KeyCode::PageUp => Some(TuiWidgetEvent::PrevPageKey),
            KeyCode::PageDown => Some(TuiWidgetEvent::NextPageKey),
            KeyCode::Left | KeyCode::Char('h') => Some(TuiWidgetEvent::LeftKey),
            KeyCode::Right | KeyCode::Char('l') => Some(TuiWidgetEvent::RightKey),
            KeyCode::Char('+') => Some(TuiWidgetEvent::PlusKey),
            KeyCode::Char('-') => Some(TuiWidgetEvent::MinusKey),
            KeyCode::Char(' ') => Some(TuiWidgetEvent::SpaceKey),
            KeyCode::Esc => Some(TuiWidgetEvent::EscapeKey),
            KeyCode::Home => Some(TuiWidgetEvent::FocusKey),
            _ => None,
        };

        if let Some(e) = event {
            self.logs_state.transition(e);
        }
        Ok(())
    }

    fn handle_editor_keys(&mut self, key_event: KeyEvent) -> color_eyre::Result<()> {
        debug!(
            "Editor key: code={:?} modifiers={:?}",
            key_event.code, key_event.modifiers
        );

        if is_execute_key_combo(&key_event) {
            if !self.query_executing {
                self.execute_query();
            }
            return Ok(());
        }

        match key_event.code {
            KeyCode::Esc => {
                self.focused_pane = if self.show_query_results
                    || matches!(self.current_view, CurrentView::TableView(_))
                {
                    FocusedPane::Results
                } else {
                    FocusedPane::Sidebar
                };
                return Ok(());
            }
            KeyCode::PageUp => {
                for _ in 0..DEFAULT_VISIBLE_ROWS {
                    self.sql_editor.move_cursor(tui_textarea::CursorMove::Up);
                }
                self.update_editor_scroll();
                return Ok(());
            }
            KeyCode::PageDown => {
                for _ in 0..DEFAULT_VISIBLE_ROWS {
                    self.sql_editor.move_cursor(tui_textarea::CursorMove::Down);
                }
                self.update_editor_scroll();
                return Ok(());
            }
            KeyCode::Home if key_event.modifiers.contains(KeyModifiers::CONTROL) => {
                self.sql_editor.move_cursor(tui_textarea::CursorMove::Top);
                self.editor_scroll_offset = 0;
                return Ok(());
            }
            KeyCode::End if key_event.modifiers.contains(KeyModifiers::CONTROL) => {
                self.sql_editor
                    .move_cursor(tui_textarea::CursorMove::Bottom);
                self.update_editor_scroll();
                return Ok(());
            }
            KeyCode::Up if key_event.modifiers.is_empty() => {
                let (row, _) = self.sql_editor.cursor();
                if row == 0 && !self.query_history.is_empty() {
                    self.navigate_history_up();
                    return Ok(());
                }
            }
            KeyCode::Down if key_event.modifiers.is_empty() => {
                let (row, _) = self.sql_editor.cursor();
                if row >= self.sql_editor.lines().len().saturating_sub(1)
                    && self.history_index.is_some()
                {
                    self.navigate_history_down();
                    return Ok(());
                }
            }
            _ => {}
        }

        self.sql_editor.input(key_event);
        self.update_editor_scroll();
        Ok(())
    }

    fn navigate_history_up(&mut self) {
        if self.query_history.is_empty() {
            return;
        }
        if self.history_index.is_none() {
            self.saved_editor_content = Some(self.sql_editor.lines().join("\n"));
        }
        let new_index = self
            .history_index
            .map_or(0, |i| (i + 1).min(self.query_history.len() - 1));
        self.history_index = Some(new_index);
        if let Some(query) = self.query_history.get(new_index) {
            self.sql_editor = TextArea::new(query.lines().map(String::from).collect());
            self.sql_editor
                .set_cursor_line_style(ratatui::style::Style::default());
        }
    }

    fn navigate_history_down(&mut self) {
        match self.history_index {
            None => {}
            Some(0) => {
                self.history_index = None;
                if let Some(content) = self.saved_editor_content.take() {
                    self.sql_editor = TextArea::new(content.lines().map(String::from).collect());
                    self.sql_editor
                        .set_cursor_line_style(ratatui::style::Style::default());
                }
            }
            Some(i) => {
                self.history_index = Some(i - 1);
                if let Some(query) = self.query_history.get(i - 1) {
                    self.sql_editor = TextArea::new(query.lines().map(String::from).collect());
                    self.sql_editor
                        .set_cursor_line_style(ratatui::style::Style::default());
                }
            }
        }
    }

    fn handle_sidebar_keys(&mut self, key_event: KeyEvent) -> color_eyre::Result<()> {
        match key_event.code {
            KeyCode::Esc | KeyCode::Char('q') => self.running = false,
            KeyCode::Up | KeyCode::Char('k') => self.tree_navigate(-1),
            KeyCode::Down | KeyCode::Char('j') => self.tree_navigate(1),
            KeyCode::Left | KeyCode::Char('h') => self.tree_collapse(),
            KeyCode::Right | KeyCode::Char('l') => self.tree_expand_or_open(),
            KeyCode::Enter | KeyCode::Char(' ') => self.handle_tree_enter(),
            KeyCode::Char('r') => self.refresh_schema(),
            KeyCode::PageUp => self.tree_navigate(-(DEFAULT_VISIBLE_ROWS as i32)),
            KeyCode::PageDown => self.tree_navigate(DEFAULT_VISIBLE_ROWS as i32),
            KeyCode::Home => {
                if let Some(first) = self.get_visible_tree_paths().first() {
                    self.tree_state.select(first.clone());
                }
            }
            KeyCode::End => {
                if let Some(last) = self.get_visible_tree_paths().last() {
                    self.tree_state.select(last.clone());
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn get_visible_tree_paths(&self) -> Vec<Vec<TreeNodeId>> {
        let Some(structure) = &self.db_structure else {
            return vec![];
        };
        let opened = self.tree_state.opened();
        let mut paths = Vec::new();

        let root_path = vec![TreeNodeId::Root];
        paths.push(root_path.clone());

        if !opened.iter().any(|p| p == &root_path) {
            return paths;
        }

        for schema in &structure.schemas {
            let schema_path = vec![TreeNodeId::Root, TreeNodeId::Schema(schema.name.clone())];
            paths.push(schema_path.clone());

            if !opened.iter().any(|p| p == &schema_path) {
                continue;
            }

            for table in &schema.tables {
                let table_path = vec![
                    TreeNodeId::Root,
                    TreeNodeId::Schema(schema.name.clone()),
                    TreeNodeId::Table {
                        schema: schema.name.clone(),
                        table: table.name.clone(),
                    },
                ];
                paths.push(table_path.clone());

                if !opened.iter().any(|p| p == &table_path) {
                    continue;
                }

                for col in &table.columns {
                    paths.push(vec![
                        TreeNodeId::Root,
                        TreeNodeId::Schema(schema.name.clone()),
                        TreeNodeId::Table {
                            schema: schema.name.clone(),
                            table: table.name.clone(),
                        },
                        TreeNodeId::Column {
                            schema: schema.name.clone(),
                            table: table.name.clone(),
                            column: col.name.clone(),
                        },
                    ]);
                }
            }
        }
        paths
    }

    fn tree_navigate(&mut self, delta: i32) {
        let paths = self.get_visible_tree_paths();
        if paths.is_empty() {
            return;
        }

        let current = self.tree_state.selected();
        let current_idx = paths.iter().position(|p| p == current).unwrap_or(0);
        let new_idx = if delta < 0 {
            current_idx.saturating_sub((-delta) as usize)
        } else {
            (current_idx + delta as usize).min(paths.len() - 1)
        };

        if let Some(new_path) = paths.get(new_idx) {
            self.tree_state.select(new_path.clone());
        }
    }

    fn tree_collapse(&mut self) {
        let selected = self.tree_state.selected().to_vec();
        if selected.is_empty() {
            return;
        }

        if self.tree_state.opened().iter().any(|p| *p == selected) {
            self.tree_state.close(&selected);
            return;
        }

        if selected.len() > 1 {
            self.tree_state
                .select(selected[..selected.len() - 1].to_vec());
        }
    }

    fn tree_expand_or_open(&mut self) {
        let selected = self.tree_state.selected().to_vec();
        if selected.is_empty() {
            return;
        }

        let is_expanded = self.tree_state.opened().iter().any(|p| *p == selected);

        match selected.last() {
            Some(TreeNodeId::Root) | Some(TreeNodeId::Schema(_)) => {
                if is_expanded {
                    self.tree_navigate(1);
                } else {
                    self.tree_state.open(selected);
                }
            }
            Some(TreeNodeId::Table { schema, table }) => {
                let (schema, table) = (schema.clone(), table.clone());
                if is_expanded {
                    self.open_schema_table(schema, table);
                    self.focused_pane = FocusedPane::Results;
                } else {
                    self.tree_state.open(selected);
                }
            }
            Some(TreeNodeId::Column { column, .. }) => {
                self.sql_editor.insert_str(column);
                self.focused_pane = FocusedPane::Editor;
            }
            None => {}
        }
    }

    fn handle_tree_enter(&mut self) {
        let selected = self.tree_state.selected().to_vec();
        if selected.is_empty() {
            return;
        }

        let is_open = self.tree_state.opened().iter().any(|p| *p == selected);

        match selected.last() {
            Some(TreeNodeId::Root) | Some(TreeNodeId::Schema(_)) => {
                if is_open {
                    self.tree_state.close(&selected);
                } else {
                    self.tree_state.open(selected);
                }
            }
            Some(TreeNodeId::Table { schema, table }) => {
                let (schema, table) = (schema.clone(), table.clone());
                if is_open {
                    self.open_schema_table(schema, table);
                    self.focused_pane = FocusedPane::Results;
                } else {
                    self.tree_state.open(selected);
                }
            }
            Some(TreeNodeId::Column { column, .. }) => {
                self.sql_editor.insert_str(column);
                self.focused_pane = FocusedPane::Editor;
            }
            None => {}
        }
    }

    fn open_schema_table(&mut self, schema: String, table: String) {
        info!("Opening table: {}.{}", schema, table);
        self.show_query_results = false;
        self.selected_table = Some((schema.clone(), table.clone()));

        let full_name = if schema == "public" {
            table
        } else {
            format!("{}.{}", schema, table)
        };

        self.current_view = CurrentView::TableView(TableViewState {
            table_name: full_name.clone(),
            columns: Vec::new(),
            rows: Vec::new(),
            total_count: 0,
            page: 0,
            selected_row: 0,
            scroll_offset: 0,
            col_offset: 0,
            loading: true,
            error: None,
        });
        self.fetch_table_data(&full_name, 0);
    }

    fn refresh_schema(&mut self) {
        let ConnectionState::Connected { pool, db_type, .. } = &self.connection else {
            return;
        };
        let pool = pool.clone();
        let db_type = *db_type;
        let sender = self.events.sender();
        tokio::spawn(async move {
            let structure = fetch_database_structure(&pool, db_type).await;
            let _ = sender.send(Event::App(AppEvent::SchemaLoaded(structure)));
        });
    }

    fn handle_results_keys(&mut self, key_event: KeyEvent) -> color_eyre::Result<()> {
        if key_event.code == KeyCode::Char('x') {
            let has_data = if self.show_query_results {
                self.query_result
                    .as_ref()
                    .is_some_and(|qr| !qr.columns.is_empty())
            } else if let CurrentView::TableView(state) = &self.current_view {
                !state.columns.is_empty()
            } else {
                false
            };
            if has_data {
                self.export_modal = true;
                return Ok(());
            }
        }

        if key_event.code == KeyCode::Char('c') && self.show_query_results {
            self.show_query_results = false;
            self.query_result = None;
            return Ok(());
        }

        if matches!(key_event.code, KeyCode::Char('b') | KeyCode::Esc)
            && matches!(self.current_view, CurrentView::TableView(_))
        {
            self.current_view = CurrentView::TableList;
            self.show_query_results = false;
            self.focused_pane = FocusedPane::Sidebar;
            return Ok(());
        }

        if key_event.code == KeyCode::Char('q') {
            self.running = false;
            return Ok(());
        }

        let visible_rows = self.results_area_height.get() as usize;

        if self.show_query_results {
            if let Some(ref mut qr) = self.query_result
                && !qr.rows.is_empty()
            {
                handle_list_navigation(
                    key_event.code,
                    &mut qr.selected_row,
                    &mut qr.scroll_offset,
                    qr.rows.len(),
                    visible_rows,
                );
            }
            // Horizontal column scroll in query results (←/→ not used for page nav here)
            if let Some(ref mut qr) = self.query_result {
                let num_cols = qr.columns.len();
                match key_event.code {
                    KeyCode::Left | KeyCode::Char('h') => {
                        qr.col_offset = qr.col_offset.saturating_sub(1);
                    }
                    KeyCode::Right | KeyCode::Char('l') if qr.col_offset + 1 < num_cols => {
                        qr.col_offset += 1;
                    }
                    _ => {}
                }
            }
        } else if let CurrentView::TableView(state) = &mut self.current_view {
            if !state.rows.is_empty() {
                handle_list_navigation(
                    key_event.code,
                    &mut state.selected_row,
                    &mut state.scroll_offset,
                    state.rows.len(),
                    visible_rows,
                );
            }

            // Shift+←/→ scrolls columns; plain ←/→ navigates pages
            let num_cols = state.columns.len();
            match (key_event.code, key_event.modifiers) {
                (KeyCode::Left, KeyModifiers::SHIFT) => {
                    state.col_offset = state.col_offset.saturating_sub(1);
                }
                (KeyCode::Right, KeyModifiers::SHIFT) if state.col_offset + 1 < num_cols => {
                    state.col_offset += 1;
                }
                _ => {}
            }

            let mut fetch_page = None;
            match key_event.code {
                KeyCode::Left | KeyCode::Char('h')
                    if key_event.modifiers == KeyModifiers::NONE
                        && state.page > 0
                        && !state.loading =>
                {
                    state.page -= 1;
                    state.loading = true;
                    state.selected_row = 0;
                    state.scroll_offset = 0;
                    state.col_offset = 0;
                    fetch_page = Some((state.table_name.clone(), state.page));
                }
                KeyCode::Right | KeyCode::Char('l')
                    if key_event.modifiers == KeyModifiers::NONE
                        && state.page < state.total_pages().saturating_sub(1)
                        && !state.loading =>
                {
                    state.page += 1;
                    state.loading = true;
                    state.selected_row = 0;
                    state.scroll_offset = 0;
                    state.col_offset = 0;
                    fetch_page = Some((state.table_name.clone(), state.page));
                }
                _ => {}
            }
            if let Some((table_name, page)) = fetch_page {
                self.fetch_table_data(&table_name, page);
            }
        }
        Ok(())
    }

    pub fn query_elapsed_ms(&self) -> Option<u128> {
        self.query_start_time.map(|t| t.elapsed().as_millis())
    }

    fn handle_export_modal_keys(&mut self, key_event: KeyEvent) -> color_eyre::Result<()> {
        match key_event.code {
            KeyCode::Char('c') => self.do_export(ExportFormat::Csv),
            KeyCode::Char('t') => self.do_export(ExportFormat::Tsv),
            KeyCode::Char('j') => self.do_export(ExportFormat::Json),
            KeyCode::Char('m') => self.do_export(ExportFormat::Markdown),
            KeyCode::Esc => self.export_modal = false,
            _ => {}
        }
        Ok(())
    }

    fn do_export(&mut self, format: ExportFormat) {
        let data = if self.show_query_results {
            self.query_result
                .as_ref()
                .filter(|qr| !qr.columns.is_empty())
                .map(|qr| (qr.columns.clone(), qr.rows.clone()))
        } else if let CurrentView::TableView(state) = &self.current_view {
            if !state.columns.is_empty() {
                Some((state.columns.clone(), state.rows.clone()))
            } else {
                None
            }
        } else {
            None
        };

        self.export_modal = false;

        let Some((columns, rows)) = data else {
            return;
        };

        let filename = format!("export.{}", format.extension());
        let result = match format {
            ExportFormat::Csv => export_csv(&filename, &columns, &rows),
            ExportFormat::Tsv => export_tsv(&filename, &columns, &rows),
            ExportFormat::Json => export_json(&filename, &columns, &rows),
            ExportFormat::Markdown => export_markdown(&filename, &columns, &rows),
        };

        self.export_message = Some((
            match result {
                Ok(()) => format!("✓ Exported to {filename}"),
                Err(e) => format!("✗ Export failed: {e}"),
            },
            Instant::now(),
        ));
    }

    pub fn export_status_text(&self) -> Option<&str> {
        self.export_message
            .as_ref()
            .filter(|(_, ts)| ts.elapsed() < Duration::from_secs(2))
            .map(|(msg, _)| msg.as_str())
    }
}

fn handle_list_navigation(
    code: KeyCode,
    selected: &mut usize,
    scroll_offset: &mut usize,
    len: usize,
    visible_rows: usize,
) {
    let page = visible_rows.max(1);
    match code {
        KeyCode::Up | KeyCode::Char('k') => {
            *selected = if *selected > 0 {
                *selected - 1
            } else {
                len - 1
            };
            if *selected == len - 1 {
                *scroll_offset = len.saturating_sub(page);
            }
        }
        KeyCode::Down | KeyCode::Char('j') => {
            *selected = if *selected < len - 1 {
                *selected + 1
            } else {
                0
            };
            if *selected == 0 {
                *scroll_offset = 0;
            }
        }
        KeyCode::PageUp => *selected = selected.saturating_sub(page),
        KeyCode::PageDown => *selected = (*selected + page).min(len - 1),
        KeyCode::Home => {
            *selected = 0;
            *scroll_offset = 0;
        }
        KeyCode::End => {
            *selected = len - 1;
            *scroll_offset = len.saturating_sub(page);
        }
        _ => return,
    }

    if *selected < *scroll_offset {
        *scroll_offset = *selected;
    }
    if *selected >= *scroll_offset + page {
        *scroll_offset = selected.saturating_sub(page - 1);
    }
}

fn csv_quote(s: &str) -> String {
    if s.contains(',') || s.contains('\n') || s.contains('"') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn export_csv(filename: &str, columns: &[String], rows: &[Vec<String>]) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(filename)?;
    writeln!(
        f,
        "{}",
        columns.iter().map(|c| csv_quote(c)).collect::<Vec<_>>().join(",")
    )?;
    for row in rows {
        writeln!(
            f,
            "{}",
            row.iter().map(|v| csv_quote(v)).collect::<Vec<_>>().join(",")
        )?;
    }
    Ok(())
}

fn export_tsv(filename: &str, columns: &[String], rows: &[Vec<String>]) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(filename)?;
    writeln!(f, "{}", columns.join("\t"))?;
    for row in rows {
        writeln!(f, "{}", row.join("\t"))?;
    }
    Ok(())
}

fn export_json(filename: &str, columns: &[String], rows: &[Vec<String>]) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(filename)?;
    let objects: Vec<String> = rows
        .iter()
        .map(|row| {
            let fields: Vec<String> = columns
                .iter()
                .zip(row.iter())
                .map(|(k, v)| {
                    format!(
                        "  \"{}\": \"{}\"",
                        k.replace('"', "\\\""),
                        v.replace('"', "\\\"")
                    )
                })
                .collect();
            format!("{{\n{}\n}}", fields.join(",\n"))
        })
        .collect();
    writeln!(f, "[\n{}\n]", objects.join(",\n"))?;
    Ok(())
}

fn export_markdown(
    filename: &str,
    columns: &[String],
    rows: &[Vec<String>],
) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(filename)?;
    writeln!(f, "| {} |", columns.join(" | "))?;
    writeln!(
        f,
        "| {} |",
        columns.iter().map(|_| "---").collect::<Vec<_>>().join(" | ")
    )?;
    for row in rows {
        writeln!(f, "| {} |", row.join(" | "))?;
    }
    Ok(())
}

fn is_execute_key_combo(key_event: &KeyEvent) -> bool {
    let ctrl = key_event.modifiers.contains(KeyModifiers::CONTROL);
    let cmd = key_event.modifiers.contains(KeyModifiers::SUPER);
    let shift = key_event.modifiers.contains(KeyModifiers::SHIFT);

    matches!(
        (key_event.code, ctrl, cmd, shift),
        (KeyCode::Enter, true, _, _)
            | (KeyCode::Enter, _, true, _)
            | (KeyCode::Enter, _, _, true)
            | (KeyCode::Char('j' | 'J'), true, _, _)
            | (KeyCode::F(5), _, _, _)
    )
}

impl Drop for App {
    fn drop(&mut self) {
        if let Some(h) = self.stats_handle.take() {
            h.abort();
        }
        if let Some(h) = self.schema_handle.take() {
            h.abort();
        }
    }
}

async fn connect_to_database(url: &str) -> Result<(AnyPool, String, DbType), String> {
    use sqlx::any::AnyConnectOptions;
    use std::str::FromStr;

    let db_type = DbType::from_url(url);

    info!(
        "Connecting to {} at {}",
        db_type.label(),
        url.split('@').next_back().unwrap_or("?")
    );
    debug!(
        "URL length={}, trimmed={}",
        url.len(),
        url.len() == url.trim().len()
    );

    let pool = match db_type {
        DbType::MySQL => {
            // Append ssl-mode=disabled to avoid the 0x49 SSLRequest error
            // with Docker MySQL's self-signed certs / disabled SSL
            let ssl_url = if url.contains('?') {
                format!("{url}&ssl-mode=disabled")
            } else {
                format!("{url}?ssl-mode=disabled")
            };

            debug!("MySQL connect URL (ssl-mode=disabled): {}", url);

            let any_opts = AnyConnectOptions::from_str(&ssl_url)
                .map_err(|e| format!("Invalid MySQL URL: {e}"))?;
            AnyPool::connect_with(any_opts).await.map_err(|e| {
                tracing::error!("MySQL connection error: {:#}", e);
                format!("{e}")
            })?
        }
        DbType::Postgres => {
            let any_opts = AnyConnectOptions::from_str(url)
                .map_err(|e| format!("Invalid Postgres URL: {e}"))?;
            AnyPool::connect_with(any_opts).await.map_err(|e| {
                tracing::error!("Postgres connection error: {:#}", e);
                format!("{e}")
            })?
        }
    };

    let db_name_query = match db_type {
        DbType::Postgres => "SELECT current_database()",
        DbType::MySQL => "SELECT DATABASE()",
    };

    let db_name = match sqlx::query_scalar::<_, Option<String>>(db_name_query)
        .fetch_one(&pool)
        .await
    {
        Ok(Some(name)) => {
            debug!("Database name query returned: {:?}", name);
            name
        }
        Ok(None) => {
            debug!("Database name query returned NULL, falling back to URL path");
            db_name_from_url(url)
        }
        Err(e) => {
            tracing::warn!(
                "Failed to query database name (decode error): {:#}. Falling back to URL.",
                e
            );
            db_name_from_url(url)
        }
    };

    info!("Connected to {} database: {}", db_type.label(), db_name);
    Ok((pool, db_name, db_type))
}

/// Extract database name from a connection URL path segment, e.g.
/// `postgres://user:pass@host:5432/mydb` → `"mydb"`.
fn db_name_from_url(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|u| {
            let path = u.path().trim_start_matches('/');
            if path.is_empty() {
                None
            } else {
                Some(path.to_string())
            }
        })
        .unwrap_or_else(|| "unknown".into())
}

/// Quote a table/identifier appropriately for the database type.
fn quote_ident(name: &str, db_type: DbType) -> String {
    match db_type {
        DbType::Postgres => format!("\"{}\"", name),
        DbType::MySQL => format!("`{}`", name),
    }
}

/// Schemas to exclude from structure queries.
fn excluded_schemas(db_type: DbType) -> &'static str {
    match db_type {
        DbType::Postgres => "('pg_catalog', 'pg_toast', 'information_schema')",
        DbType::MySQL => "('information_schema', 'performance_schema', 'mysql', 'sys')",
    }
}

async fn fetch_database_structure(pool: &AnyPool, db_type: DbType) -> DatabaseStructure {
    let excl = excluded_schemas(db_type);

    let schema_query = match db_type {
        DbType::Postgres => format!(
            r#"SELECT schema_name::text FROM information_schema.schemata
               WHERE schema_name NOT IN {excl}
               ORDER BY CASE WHEN schema_name = 'public' THEN 0 ELSE 1 END, schema_name"#
        ),
        DbType::MySQL => format!(
            r#"SELECT schema_name FROM information_schema.schemata
               WHERE schema_name NOT IN {excl}
               ORDER BY schema_name"#
        ),
    };

    let schemas: Vec<String> = sqlx::query_scalar(&schema_query)
        .fetch_all(pool)
        .await
        .unwrap_or_else(|_| match db_type {
            DbType::Postgres => vec!["public".to_string()],
            DbType::MySQL => vec![],
        });

    let table_query = match db_type {
        DbType::Postgres => format!(
            r#"SELECT table_schema::text, table_name::text FROM information_schema.tables
               WHERE table_type = 'BASE TABLE'
                 AND table_schema NOT IN {excl}
               ORDER BY table_schema, table_name"#
        ),
        DbType::MySQL => format!(
            r#"SELECT table_schema, table_name FROM information_schema.tables
               WHERE table_type = 'BASE TABLE'
                 AND table_schema NOT IN {excl}
               ORDER BY table_schema, table_name"#
        ),
    };

    let table_rows = sqlx::query(&table_query)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

    let tables: Vec<(String, String)> = table_rows
        .iter()
        .map(|r| {
            (
                r.try_get::<String, _>(0).unwrap_or_default(),
                r.try_get::<String, _>(1).unwrap_or_default(),
            )
        })
        .collect();

    let col_query = match db_type {
        DbType::Postgres => format!(
            r#"SELECT c.table_schema::text, c.table_name::text, c.column_name::text,
                      c.data_type::text, c.is_nullable::text, c.ordinal_position
               FROM information_schema.columns c
               WHERE c.table_schema NOT IN {excl}
               ORDER BY c.table_schema, c.table_name, c.ordinal_position"#
        ),
        DbType::MySQL => format!(
            r#"SELECT c.table_schema, c.table_name, c.column_name,
                      c.data_type, c.is_nullable, CAST(c.ordinal_position AS SIGNED)
               FROM information_schema.columns c
               WHERE c.table_schema NOT IN {excl}
               ORDER BY c.table_schema, c.table_name, c.ordinal_position"#
        ),
    };

    let col_rows = sqlx::query(&col_query)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

    let columns: Vec<(String, String, String, String, String, i32)> = col_rows
        .iter()
        .map(|r| {
            (
                r.try_get::<String, _>(0).unwrap_or_default(),
                r.try_get::<String, _>(1).unwrap_or_default(),
                r.try_get::<String, _>(2).unwrap_or_default(),
                r.try_get::<String, _>(3).unwrap_or_default(),
                r.try_get::<String, _>(4).unwrap_or_default(),
                r.try_get::<i32, _>(5).unwrap_or(0),
            )
        })
        .collect();

    let pk_query = match db_type {
        DbType::Postgres => format!(
            r#"SELECT tc.table_schema::text, tc.table_name::text, kcu.column_name::text
               FROM information_schema.table_constraints tc
               JOIN information_schema.key_column_usage kcu
                   ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
               WHERE tc.constraint_type = 'PRIMARY KEY'
                 AND tc.table_schema NOT IN {excl}"#
        ),
        DbType::MySQL => format!(
            r#"SELECT tc.table_schema, tc.table_name, kcu.column_name
               FROM information_schema.table_constraints tc
               JOIN information_schema.key_column_usage kcu
                   ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
               WHERE tc.constraint_type = 'PRIMARY KEY'
                 AND tc.table_schema NOT IN {excl}"#
        ),
    };

    let pk_rows = sqlx::query(&pk_query)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

    let pk_columns: Vec<(String, String, String)> = pk_rows
        .iter()
        .map(|r| {
            (
                r.try_get::<String, _>(0).unwrap_or_default(),
                r.try_get::<String, _>(1).unwrap_or_default(),
                r.try_get::<String, _>(2).unwrap_or_default(),
            )
        })
        .collect();

    use std::collections::{HashMap, HashSet};

    let pk_set: HashSet<_> = pk_columns.into_iter().collect();
    let mut schema_map: HashMap<String, Vec<DbTable>> =
        schemas.iter().map(|s| (s.clone(), Vec::new())).collect();
    let mut table_map: HashMap<(String, String), Vec<DbColumn>> = tables
        .iter()
        .map(|(s, t)| ((s.clone(), t.clone()), Vec::new()))
        .collect();

    for (schema, table, col_name, data_type, is_nullable, ordinal) in columns {
        let col = DbColumn {
            name: col_name.clone(),
            data_type: format_data_type(&data_type),
            is_nullable: is_nullable == "YES",
            is_primary_key: pk_set.contains(&(schema.clone(), table.clone(), col_name)),
            ordinal_position: ordinal,
        };
        if let Some(cols) = table_map.get_mut(&(schema, table)) {
            cols.push(col);
        }
    }

    for (schema, table) in tables {
        let columns = table_map
            .remove(&(schema.clone(), table.clone()))
            .unwrap_or_default();
        if let Some(tables) = schema_map.get_mut(&schema) {
            tables.push(DbTable {
                name: table,
                columns,
            });
        }
    }

    let db_schemas: Vec<DbSchema> = schemas
        .into_iter()
        .map(|name| DbSchema {
            tables: schema_map.remove(&name).unwrap_or_default(),
            name,
        })
        .collect();

    DatabaseStructure {
        schemas: db_schemas,
    }
}

fn format_data_type(data_type: &str) -> String {
    match data_type {
        "character varying" => "varchar".into(),
        "character" => "char".into(),
        "timestamp without time zone" => "timestamp".into(),
        "timestamp with time zone" => "timestamptz".into(),
        "double precision" => "float8".into(),
        "boolean" => "bool".into(),
        _ => data_type.into(),
    }
}

async fn fetch_stats(pool: &AnyPool, db_type: DbType) -> Option<StatsUpdate> {
    let version: String = sqlx::query_scalar("SELECT version()")
        .fetch_one(pool)
        .await
        .ok()
        .map(|v: String| v.split_whitespace().take(2).collect::<Vec<_>>().join(" "))
        .unwrap_or_else(|| "Unknown".into());

    let total_rows: i64 = match db_type {
        DbType::Postgres => {
            sqlx::query_scalar::<_, i64>(
                r#"SELECT COALESCE(SUM(n_live_tup), 0)::bigint FROM pg_stat_user_tables WHERE schemaname = 'public'"#,
            )
            .fetch_one(pool)
            .await
            .unwrap_or(0)
        }
        DbType::MySQL => {
            sqlx::query_scalar::<_, i64>(
                r#"SELECT IFNULL(SUM(table_rows), 0)
                   FROM information_schema.tables
                   WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'"#,
            )
            .fetch_one(pool)
            .await
            .unwrap_or(0)
        }
    };

    Some(StatsUpdate {
        pg_version: version,
        total_rows,
    })
}

async fn fetch_table_page(
    pool: &AnyPool,
    table_name: &str,
    page: usize,
    db_type: DbType,
) -> Result<TableDataResult, String> {
    let offset = page * PAGE_SIZE;
    let quoted = quote_ident(table_name, db_type);

    let count_row = sqlx::query(&format!("SELECT COUNT(*) FROM {quoted}"))
        .fetch_one(pool)
        .await
        .map_err(|e| format!("Failed to get row count: {e}"))?;
    let total_count: i64 = count_row.try_get(0).unwrap_or(0);

    // Split "schema.table" or default to ("public", table_name)
    let (schema_name, bare_table) = table_name
        .split_once('.')
        .unwrap_or(("public", table_name));

    // Fetch column names so we can build a text-cast SELECT.
    // This avoids AnyPool type-decoding failures for DATE, NUMERIC, UUID, etc.
    let col_name_query = match db_type {
        DbType::Postgres => format!(
            "SELECT column_name::text FROM information_schema.columns \
             WHERE table_schema = '{schema_name}' AND table_name = '{bare_table}' \
             ORDER BY ordinal_position"
        ),
        DbType::MySQL => format!(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = DATABASE() AND table_name = '{bare_table}' \
             ORDER BY ordinal_position"
        ),
    };

    let col_rows = sqlx::query(&col_name_query)
        .fetch_all(pool)
        .await
        .map_err(|e| format!("Failed to get column info: {e}"))?;

    let columns: Vec<String> = col_rows
        .iter()
        .map(|r| r.try_get::<String, _>(0).unwrap_or_default())
        .filter(|s| !s.is_empty())
        .collect();

    if columns.is_empty() {
        return Ok(TableDataResult {
            table_name: table_name.to_string(),
            columns: vec![],
            rows: vec![],
            total_count,
            page,
        });
    }

    // Build SELECT with explicit text casts for every column
    let select_cols = columns
        .iter()
        .map(|col| match db_type {
            DbType::Postgres => format!("\"{}\"::text", col),
            DbType::MySQL => format!("CAST(`{}` AS CHAR)", col),
        })
        .collect::<Vec<_>>()
        .join(", ");

    let rows = sqlx::query(&format!(
        "SELECT {select_cols} FROM {quoted} LIMIT {PAGE_SIZE} OFFSET {offset}"
    ))
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to fetch data: {e}"))?;

    let string_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row_to_strings(row, columns.len()))
        .collect();

    Ok(TableDataResult {
        table_name: table_name.to_string(),
        columns,
        rows: string_rows,
        total_count,
        page,
    })
}

async fn execute_sql_query(pool: &AnyPool, query: &str) -> Result<QueryResult, String> {
    let start = Instant::now();
    let is_explain = query.trim().to_uppercase().starts_with("EXPLAIN");

    let rows = sqlx::query(query)
        .fetch_all(pool)
        .await
        .map_err(|e| format!("{e}"))?;
    let duration_ms = start.elapsed().as_millis();

    let columns: Vec<String> = rows
        .first()
        .map(|r| r.columns().iter().map(|c| c.name().to_string()).collect())
        .unwrap_or_default();

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

fn row_to_strings(row: &sqlx::any::AnyRow, col_count: usize) -> Vec<String> {
    (0..col_count)
        .map(|i| {
            row.try_get::<String, _>(i)
                .or_else(|_| row.try_get::<i64, _>(i).map(|v| v.to_string()))
                .or_else(|_| row.try_get::<i32, _>(i).map(|v| v.to_string()))
                .or_else(|_| row.try_get::<f64, _>(i).map(|v| v.to_string()))
                .or_else(|_| row.try_get::<bool, _>(i).map(|v| v.to_string()))
                .or_else(|_| {
                    row.try_get::<Option<String>, _>(i)
                        .map(|v| v.unwrap_or_else(|| "NULL".into()))
                })
                .or_else(|_| {
                    row.try_get::<Option<i64>, _>(i)
                        .map(|v| v.map_or("NULL".into(), |n| n.to_string()))
                })
                .or_else(|_| {
                    row.try_get::<Option<i32>, _>(i)
                        .map(|v| v.map_or("NULL".into(), |n| n.to_string()))
                })
                .or_else(|_| {
                    row.try_get::<Option<f64>, _>(i)
                        .map(|v| v.map_or("NULL".into(), |n| n.to_string()))
                })
                .or_else(|_| {
                    row.try_get::<Option<bool>, _>(i)
                        .map(|v| v.map_or("NULL".into(), |b| b.to_string()))
                })
                .unwrap_or_else(|_| "<?>".into())
        })
        .collect()
}
