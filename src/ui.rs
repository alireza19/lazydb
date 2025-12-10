use ratatui::{
    buffer::Buffer,
    layout::{Alignment, Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{
        Block, BorderType, Cell, List, ListItem, ListState, Paragraph, Row, Table, Widget, Wrap,
    },
};

use crate::app::{App, ConnectionState, CurrentView, FocusedPane, QueryResultState, TableViewState};
use crate::dotline::{make_color_fn, AsciiDotGraph};

/// SQL keywords for syntax highlighting.
const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN", "IS", "NULL",
    "ORDER", "BY", "ASC", "DESC", "LIMIT", "OFFSET", "GROUP", "HAVING", "JOIN", "LEFT",
    "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "ON", "AS", "DISTINCT", "COUNT", "SUM",
    "AVG", "MIN", "MAX", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "CREATE",
    "TABLE", "DROP", "ALTER", "INDEX", "VIEW", "TRIGGER", "FUNCTION", "PROCEDURE", "BEGIN",
    "END", "IF", "ELSE", "THEN", "CASE", "WHEN", "ELSE", "COALESCE", "NULLIF", "CAST",
    "UNION", "ALL", "INTERSECT", "EXCEPT", "EXISTS", "ANY", "SOME", "EXPLAIN", "ANALYZE",
    "WITH", "RECURSIVE", "RETURNING", "CONFLICT", "DO", "NOTHING", "TRUE", "FALSE",
];

impl Widget for &App {
    fn render(self, area: Rect, buf: &mut Buffer) {
        match &self.current_view {
            CurrentView::ConnectionStatus => render_connection_status(self, area, buf),
            _ => render_main_layout(self, area, buf),
        }
    }
}

/// Render the connection status view (connecting or failed).
fn render_connection_status(app: &App, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered()
        .title(" lazydb ")
        .title_alignment(Alignment::Center)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = block.inner(area);
    block.render(area, buf);

    let layout = Layout::vertical([
        Constraint::Fill(1),
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Fill(1),
    ])
    .split(inner);

    let status_line = match &app.connection {
        ConnectionState::Connecting => Line::from(vec![
            Span::styled("⟳ ", Style::default().fg(Color::Yellow)),
            Span::styled("Connecting...", Style::default().fg(Color::Yellow)),
        ]),
        ConnectionState::Connected { db_name, .. } => Line::from(vec![
            Span::styled("● ", Style::default().fg(Color::Green)),
            Span::styled(
                format!("Connected to {db_name}"),
                Style::default().fg(Color::Green),
            ),
        ]),
        ConnectionState::Failed { error } => Line::from(vec![
            Span::styled("✗ ", Style::default().fg(Color::Red)),
            Span::styled(
                format!("Connection failed: {error}"),
                Style::default().fg(Color::Red),
            ),
        ]),
    };

    Paragraph::new(status_line)
        .alignment(Alignment::Center)
        .render(layout[1], buf);

    let help_line = Line::from(vec![
        Span::styled("Press ", Style::default().fg(Color::DarkGray)),
        Span::styled("q", Style::default().fg(Color::White).bold()),
        Span::styled(" to quit", Style::default().fg(Color::DarkGray)),
    ]);

    Paragraph::new(help_line)
        .alignment(Alignment::Center)
        .render(layout[2], buf);
}

/// Render the main 4-pane layout.
fn render_main_layout(app: &App, area: Rect, buf: &mut Buffer) {
    let main_block = Block::bordered()
        .title(" lazydb ")
            .title_alignment(Alignment::Center)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = main_block.inner(area);
    main_block.render(area, buf);

    // Split into main content and global status bar at bottom
    let outer_layout = Layout::vertical([
        Constraint::Min(1),    // Main content
        Constraint::Length(1), // Global status bar
    ])
    .split(inner);

    let content_area = outer_layout[0];
    let status_bar_area = outer_layout[1];

    // Split content into top (70%) and bottom (30%)
    let vertical_layout = Layout::vertical([
        Constraint::Percentage(70),
        Constraint::Percentage(30),
    ])
    .split(content_area);

    let top_area = vertical_layout[0];
    let bottom_area = vertical_layout[1];

    // Top: horizontal split (30% sidebar + 70% results)
    let top_horizontal = Layout::horizontal([
        Constraint::Percentage(30),
        Constraint::Percentage(70),
    ])
    .split(top_area);

    // Bottom: horizontal split (30% stats + 70% SQL editor)
    let bottom_horizontal = Layout::horizontal([
        Constraint::Percentage(30),
        Constraint::Percentage(70),
    ])
    .split(bottom_area);

    render_sidebar(app, top_horizontal[0], buf);
    render_content_area(app, top_horizontal[1], buf);
    render_stats_panel(app, bottom_horizontal[0], buf);
    render_sql_editor(app, bottom_horizontal[1], buf);

    // Render global status bar
    render_global_status_bar(app, status_bar_area, buf);
}

/// Render the global status bar at the bottom.
fn render_global_status_bar(app: &App, area: Rect, buf: &mut Buffer) {
    let status = Line::from(vec![
        Span::styled(
            format!("[{}]", app.focused_pane.label()),
            Style::default().fg(Color::Yellow).bold(),
        ),
        Span::styled(" │ ", Style::default().fg(Color::Rgb(60, 60, 60))),
        Span::styled("Tab", Style::default().fg(Color::Cyan)),
        Span::styled(" cycle  ", Style::default().fg(Color::DarkGray)),
        Span::styled(":", Style::default().fg(Color::Cyan)),
        Span::styled(" SQL  ", Style::default().fg(Color::DarkGray)),
        Span::styled("q", Style::default().fg(Color::Cyan)),
        Span::styled(" quit", Style::default().fg(Color::DarkGray)),
    ]);

    Paragraph::new(status)
        .alignment(Alignment::Center)
        .render(area, buf);
}

/// Render the left sidebar with table list.
fn render_sidebar(app: &App, area: Rect, buf: &mut Buffer) {
    let db_name = match &app.connection {
        ConnectionState::Connected { db_name, .. } => db_name.as_str(),
        _ => "database",
    };

    let viewing_table = match &app.current_view {
        CurrentView::TableView(state) => Some(state.table_name.as_str()),
        _ => None,
    };

    let is_focused = app.focused_pane == FocusedPane::Sidebar;
    let border_color = if is_focused {
        Color::Yellow
    } else {
        Color::Rgb(60, 60, 60)
    };

    let sidebar_block = Block::bordered()
        .title(format!(" {} ", db_name))
        .title_style(Style::default().fg(Color::Cyan).bold())
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(border_color));

    let sidebar_inner = sidebar_block.inner(area);
    sidebar_block.render(area, buf);

    // Split for list and footer
    let layout = Layout::vertical([
        Constraint::Min(1),
        Constraint::Length(1),
    ])
    .split(sidebar_inner);

    let list_area = layout[0];
    let footer_area = layout[1];

    if app.tables.is_empty() {
        let empty_text = Paragraph::new(Line::from(vec![Span::styled(
            "<empty>",
            Style::default().fg(Color::DarkGray).italic(),
        )]))
        .alignment(Alignment::Center);

        let centered_layout = Layout::vertical([
            Constraint::Fill(1),
            Constraint::Length(1),
            Constraint::Fill(1),
        ])
        .split(list_area);

        empty_text.render(centered_layout[1], buf);
    } else {
        let items: Vec<ListItem> = app
            .tables
            .iter()
            .enumerate()
            .map(|(i, table)| {
                let is_selected = i == app.selected_table_index;
                let is_viewing = viewing_table == Some(table.as_str());

                let prefix = if is_selected { "▸ " } else { "  " };
                let style = if is_viewing {
                    Style::default()
                        .fg(Color::Green)
                        .add_modifier(Modifier::BOLD)
                } else if is_selected {
                    Style::default()
            .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::White)
                };

                ListItem::new(Line::from(vec![
                    Span::styled(prefix, style),
                    Span::styled(format!("󰓫 {}", table), style),
                ]))
            })
            .collect();

        let list = List::new(items);
        let mut state = ListState::default();
        state.select(Some(app.selected_table_index));

        ratatui::widgets::StatefulWidget::render(list, list_area, buf, &mut state);
    }

    // Footer
    let footer = Line::from(vec![Span::styled(
        format!("{} tables", app.tables.len()),
        Style::default().fg(Color::DarkGray),
    )]);

    Paragraph::new(footer)
        .alignment(Alignment::Center)
        .render(footer_area, buf);
}

/// Render the live ASCII dot-scatter dashboard (bottom-left).
fn render_stats_panel(app: &App, area: Rect, buf: &mut Buffer) {
    let is_focused = app.focused_pane == FocusedPane::Stats;
    let border_color = if is_focused {
        Color::Yellow
    } else {
        Color::Rgb(60, 60, 60)
    };

    let block = Block::bordered()
        .title(" ◉ Live Monitor ")
        .title_style(Style::default().fg(Color::Magenta).bold())
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(border_color));

    let inner = block.inner(area);
    block.render(area, buf);

    // Split into graphs area (top) and info area (bottom)
    let layout = Layout::vertical([
        Constraint::Min(4),    // Graphs
        Constraint::Length(3), // Info text
    ])
    .split(inner);

    let graphs_area = layout[0];
    let info_area = layout[1];

    // Render 4 ASCII dot-scatter graphs in a 2x2 grid
    render_ascii_graphs(app, graphs_area, buf);

    // Render info text
    render_stats_info(app, info_area, buf);
}

/// Render the 4 ASCII dot-scatter graphs in a 2x2 grid layout.
fn render_ascii_graphs(app: &App, area: Rect, buf: &mut Buffer) {
    // Split into 2 rows
    let rows = Layout::vertical([
        Constraint::Ratio(1, 2),
        Constraint::Ratio(1, 2),
    ])
    .split(area);

    // Split each row into 2 columns
    let top_cols = Layout::horizontal([
        Constraint::Ratio(1, 2),
        Constraint::Ratio(1, 2),
    ])
    .split(rows[0]);

    let bottom_cols = Layout::horizontal([
        Constraint::Ratio(1, 2),
        Constraint::Ratio(1, 2),
    ])
    .split(rows[1]);

    // Color functions for each metric
    let qps_color = make_color_fn(50, true);
    let rows_color = make_color_fn(10000, true);
    let latency_color_fn = make_color_fn(300, false);
    let conn_color = make_color_fn(20, true);

    // Top-left: Queries/sec
    render_ascii_graph_cell(
        "qps",
        &app.stats.queries_per_sec,
        &qps_color,
        top_cols[0],
        buf,
    );

    // Top-right: Rows/sec
    render_ascii_graph_cell(
        "rows",
        &app.stats.rows_per_sec,
        &rows_color,
        top_cols[1],
        buf,
    );

    // Bottom-left: Latency
    render_ascii_graph_cell(
        "ms",
        &app.stats.latency_ms,
        &latency_color_fn,
        bottom_cols[0],
        buf,
    );

    // Bottom-right: Connections
    render_ascii_graph_cell(
        "conn",
        &app.stats.connections,
        &conn_color,
        bottom_cols[1],
        buf,
    );
}

/// Render a single ASCII dot-scatter graph cell with label.
fn render_ascii_graph_cell<F>(
    label: &str,
    data: &std::collections::VecDeque<u64>,
    color_fn: &F,
    area: Rect,
    buf: &mut Buffer,
)
where
    F: Fn(u64, u64) -> Color,
{
    if area.height == 0 || area.width == 0 {
        return;
    }

    // Split into header (label + value) and graph area
    let layout = Layout::vertical([
        Constraint::Length(1), // Header
        Constraint::Min(1),    // Graph
    ])
    .split(area);

    let header_area = layout[0];
    let graph_area = layout[1];

    // Get current value and max for display
    let current_value = data.back().copied().unwrap_or(0);
    let observed_max = data.iter().max().copied().unwrap_or(1).max(1);
    let current_color = color_fn(current_value, observed_max);

    // Render header: label on left, value on right
    let header_line = Line::from(vec![
        Span::styled(format!(" {} ", label), Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{:>4}", current_value),
            Style::default().fg(current_color).bold(),
        ),
    ]);
    Paragraph::new(header_line).render(header_area, buf);

    // Render ASCII dot-scatter graph
    let graph_height = graph_area.height.max(1);
    let graph = AsciiDotGraph::new(data, observed_max, color_fn).height(graph_height);
    graph.render(graph_area, buf);
}

/// Render the stats info text below graphs.
fn render_stats_info(app: &App, area: Rect, buf: &mut Buffer) {
    let lines = vec![
        Line::from(vec![
            Span::styled("● ", Style::default().fg(Color::Rgb(80, 255, 80))),
            Span::styled(&app.stats.host, Style::default().fg(Color::White)),
            Span::styled(" │ ", Style::default().fg(Color::Rgb(60, 60, 60))),
            Span::styled(&app.stats.database, Style::default().fg(Color::Cyan).bold()),
        ]),
        Line::from(vec![
            Span::styled("Tables: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}", app.stats.table_count),
                Style::default().fg(Color::White),
            ),
            Span::styled(" │ ", Style::default().fg(Color::Rgb(60, 60, 60))),
            Span::styled("Last: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                app.stats
                    .last_query_ms
                    .map_or("—".to_string(), |ms| format!("{}ms", ms)),
                latency_color(app.stats.last_query_ms.unwrap_or(0) as u64),
            ),
            Span::styled(" │ ", Style::default().fg(Color::Rgb(60, 60, 60))),
            Span::styled("Total: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}", app.stats.queries_run),
                Style::default().fg(Color::White),
            ),
        ]),
        Line::from(vec![
            Span::styled(
                if app.stats.pg_version.is_empty() {
                    "PostgreSQL".to_string()
                } else {
                    app.stats.pg_version.clone()
                },
                Style::default().fg(Color::Rgb(80, 80, 80)).italic(),
            ),
        ]),
    ];

    Paragraph::new(lines).render(area, buf);
}

/// Get color for latency value.
fn latency_color(ms: u64) -> Style {
    let color = if ms == 0 {
        Color::DarkGray
    } else if ms < 100 {
        Color::Rgb(80, 255, 80)   // Green
    } else if ms < 200 {
        Color::Rgb(255, 255, 0)   // Yellow
    } else if ms < 300 {
        Color::Rgb(255, 165, 0)   // Orange
    } else {
        Color::Rgb(255, 80, 80)   // Red
    };
    Style::default().fg(color)
}


/// Render the right content area.
fn render_content_area(app: &App, area: Rect, buf: &mut Buffer) {
    if app.show_query_results {
        if let Some(ref qr) = app.query_result {
            render_query_results(qr, app, area, buf);
        }
    } else {
        match &app.current_view {
            CurrentView::TableView(state) => render_table_view(state, app, area, buf),
            _ => render_placeholder(app, area, buf),
        }
    }
}

/// Render placeholder when no table is selected.
fn render_placeholder(app: &App, area: Rect, buf: &mut Buffer) {
    let is_focused = app.focused_pane == FocusedPane::Results;
    let border_color = if is_focused {
        Color::Yellow
    } else {
        Color::Rgb(60, 60, 60)
    };

    let content_block = Block::bordered()
        .title(" Results ")
        .title_style(Style::default().fg(Color::Magenta).bold())
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(border_color));

    let content_inner = content_block.inner(area);
    content_block.render(area, buf);

    let centered_layout = Layout::vertical([
        Constraint::Fill(1),
        Constraint::Length(1),
        Constraint::Length(2),
        Constraint::Fill(1),
    ])
    .split(content_inner);

    let placeholder = Paragraph::new(Line::from(vec![
        Span::styled("Select a table ", Style::default().fg(Color::DarkGray)),
        Span::styled("→", Style::default().fg(Color::Cyan)),
    ]))
    .alignment(Alignment::Center);

    placeholder.render(centered_layout[1], buf);

    let help_line = Line::from(vec![
        Span::styled("↑↓", Style::default().fg(Color::Cyan)),
        Span::styled(" navigate  ", Style::default().fg(Color::DarkGray)),
        Span::styled("Enter", Style::default().fg(Color::Cyan)),
        Span::styled(" select  ", Style::default().fg(Color::DarkGray)),
        Span::styled(":", Style::default().fg(Color::Cyan)),
        Span::styled(" SQL  ", Style::default().fg(Color::DarkGray)),
        Span::styled("q", Style::default().fg(Color::Cyan)),
        Span::styled(" quit", Style::default().fg(Color::DarkGray)),
    ]);

    Paragraph::new(help_line)
        .alignment(Alignment::Center)
        .render(centered_layout[2], buf);
}

/// Render the table data view.
fn render_table_view(state: &TableViewState, app: &App, area: Rect, buf: &mut Buffer) {
    let is_focused = app.focused_pane == FocusedPane::Results;
    let border_color = if is_focused {
        Color::Yellow
    } else {
        Color::Rgb(60, 60, 60)
    };

    let content_block = Block::bordered()
        .title(format!(" {} ", state.table_name))
        .title_style(Style::default().fg(Color::Magenta).bold())
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(border_color));

    let content_inner = content_block.inner(area);
    content_block.render(area, buf);

    let layout = Layout::vertical([
        Constraint::Min(3),
        Constraint::Length(1),
    ])
    .split(content_inner);

    let table_area = layout[0];
    let footer_area = layout[1];

    if state.loading {
        render_centered_message(table_area, buf, "⟳ ", "Loading...", Color::Yellow);
    } else if let Some(error) = &state.error {
        render_centered_message(table_area, buf, "✗ ", error, Color::Red);
    } else if state.rows.is_empty() {
        render_centered_message(table_area, buf, "", "<empty table>", Color::DarkGray);
    } else {
        render_data_table(&state.columns, &state.rows, state.selected_row, state.scroll_offset, table_area, buf);
    }

    render_table_footer(state, app, footer_area, buf);
}

/// Render query results.
fn render_query_results(qr: &QueryResultState, app: &App, area: Rect, buf: &mut Buffer) {
    let is_focused = app.focused_pane == FocusedPane::Results;
    let border_color = if is_focused {
        Color::Yellow
    } else {
        Color::Rgb(60, 60, 60)
    };

    let title = if qr.error.is_some() {
        " Query Error "
    } else {
        " Query Results "
    };

    let content_block = Block::bordered()
        .title(title)
        .title_style(Style::default().fg(Color::Magenta).bold())
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(border_color));

    let content_inner = content_block.inner(area);
    content_block.render(area, buf);

    let layout = Layout::vertical([
        Constraint::Min(3),
        Constraint::Length(1),
    ])
    .split(content_inner);

    let table_area = layout[0];
    let footer_area = layout[1];

    if let Some(error) = &qr.error {
        // Render error message
        let error_para = Paragraph::new(error.clone())
            .style(Style::default().fg(Color::Red))
            .wrap(Wrap { trim: false });
        error_para.render(table_area, buf);
    } else if qr.rows.is_empty() {
        if qr.columns.is_empty() {
            render_centered_message(table_area, buf, "✓ ", "Query executed successfully", Color::Green);
        } else {
            render_centered_message(table_area, buf, "", "<no rows returned>", Color::DarkGray);
        }
    } else if qr.is_explain {
        // Render EXPLAIN as text
        render_explain_results(qr, table_area, buf);
    } else {
        render_data_table(&qr.columns, &qr.rows, qr.selected_row, qr.scroll_offset, table_area, buf);
    }

    // Footer
    let footer = Line::from(vec![
        Span::styled(
            format!("{} rows", qr.row_count),
            Style::default().fg(Color::Cyan),
        ),
        Span::styled(" │ ", Style::default().fg(Color::Rgb(60, 60, 60))),
        Span::styled(
            format!("{}ms", qr.duration_ms),
            Style::default().fg(Color::Green),
        ),
        Span::styled(" │ ", Style::default().fg(Color::Rgb(60, 60, 60))),
        Span::styled("c", Style::default().fg(Color::Cyan)),
        Span::styled(" clear", Style::default().fg(Color::DarkGray)),
    ]);

    Paragraph::new(footer)
        .alignment(Alignment::Center)
        .render(footer_area, buf);
}

/// Render EXPLAIN results as a tree-like text.
fn render_explain_results(qr: &QueryResultState, area: Rect, buf: &mut Buffer) {
    let lines: Vec<Line> = qr.rows.iter().map(|row| {
        let text = row.first().map(|s| s.as_str()).unwrap_or("");
        Line::from(Span::styled(text, Style::default().fg(Color::White)))
    }).collect();

    let para = Paragraph::new(lines)
        .style(Style::default())
        .wrap(Wrap { trim: false });

    para.render(area, buf);
}

/// Render a centered message.
fn render_centered_message(area: Rect, buf: &mut Buffer, prefix: &str, msg: &str, color: Color) {
    let centered = Layout::vertical([
        Constraint::Fill(1),
        Constraint::Length(1),
        Constraint::Fill(1),
    ])
    .split(area);

    let line = Line::from(vec![
        Span::styled(prefix, Style::default().fg(color)),
        Span::styled(msg, Style::default().fg(color)),
    ]);

    Paragraph::new(line)
        .alignment(Alignment::Center)
        .render(centered[1], buf);
}

/// Render the actual data table with scrolling support.
fn render_data_table(
    columns: &[String],
    rows: &[Vec<String>],
    selected_row: usize,
    scroll_offset: usize,
    area: Rect,
    buf: &mut Buffer,
) {
    let col_count = columns.len();
    if col_count == 0 {
        return;
    }

    // Calculate column widths based on content
    let mut col_widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < col_widths.len() {
                col_widths[i] = col_widths[i].max(cell.len().min(30));
            }
        }
    }

    let constraints: Vec<Constraint> = col_widths
        .iter()
        .map(|&w| Constraint::Length((w + 2) as u16))
        .collect();

    // Header
    let header_cells: Vec<Cell> = columns
        .iter()
        .map(|col| {
            Cell::from(col.clone())
                .style(Style::default().fg(Color::Cyan).bold())
        })
        .collect();
    let header = Row::new(header_cells)
        .style(Style::default())
        .height(1);

    // Calculate visible area (height minus header)
    let visible_rows = area.height.saturating_sub(1) as usize;

    // Get the visible slice of rows
    let end_idx = (scroll_offset + visible_rows).min(rows.len());
    let visible_slice = &rows[scroll_offset..end_idx];

    // Build visible rows with correct selection highlighting
    let data_rows: Vec<Row> = visible_slice
        .iter()
        .enumerate()
        .map(|(visible_idx, row)| {
            let actual_idx = scroll_offset + visible_idx;
            let is_selected = actual_idx == selected_row;

            let cells: Vec<Cell> = row
                .iter()
                .map(|cell| {
                    let display = if cell.len() > 30 {
                        format!("{}…", &cell[..29])
                    } else {
                        cell.clone()
                    };

                    let style = if cell == "NULL" {
                        Style::default().fg(Color::DarkGray).italic()
                    } else {
                        Style::default().fg(Color::White)
                    };

                    Cell::from(display).style(style)
                })
                .collect();

            let row_style = if is_selected {
                Style::default().bg(Color::Rgb(40, 40, 60))
            } else {
                Style::default()
            };

            Row::new(cells).style(row_style).height(1)
        })
        .collect();

    let table = Table::new(data_rows, constraints)
        .header(header)
        .row_highlight_style(Style::default().bg(Color::Rgb(40, 40, 60)));

    Widget::render(table, area, buf);
}

/// Calculate visible row count for a given area (minus header).
pub fn visible_row_count(area: Rect) -> usize {
    area.height.saturating_sub(1) as usize
}

/// Render the table view footer.
fn render_table_footer(state: &TableViewState, _app: &App, area: Rect, buf: &mut Buffer) {
    let total_pages = state.total_pages();
    let current_page = state.page + 1;

    let spans = vec![
        Span::styled("Page ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{}", current_page),
            Style::default().fg(Color::Cyan),
        ),
        Span::styled(
            format!("/{}", total_pages),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(" │ ", Style::default().fg(Color::Rgb(60, 60, 60))),
        Span::styled("Rows: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("~{}", state.total_count),
            Style::default().fg(Color::Cyan),
        ),
        Span::styled(" │ ", Style::default().fg(Color::Rgb(60, 60, 60))),
        Span::styled("←→", Style::default().fg(Color::Cyan)),
        Span::styled(" page  ", Style::default().fg(Color::DarkGray)),
        Span::styled("↑↓", Style::default().fg(Color::Cyan)),
        Span::styled(" row", Style::default().fg(Color::DarkGray)),
    ];

    let footer = Line::from(spans);

    Paragraph::new(footer)
        .alignment(Alignment::Center)
        .render(area, buf);
}

/// Render the SQL editor.
fn render_sql_editor(app: &App, area: Rect, buf: &mut Buffer) {
    let is_focused = app.focused_pane == FocusedPane::Editor;
    let border_color = if is_focused {
        Color::Yellow
    } else {
        Color::Rgb(60, 60, 60)
    };

    let title = if app.query_executing {
        format!(" SQL ⟳ {}ms ", app.query_elapsed_ms().unwrap_or(0))
    } else if is_focused {
        " SQL [editing] ".to_string()
    } else {
        " SQL ".to_string()
    };

    let block = Block::bordered()
        .title(title)
        .title_style(if is_focused {
            Style::default().fg(Color::Yellow).bold()
        } else {
            Style::default().fg(Color::Rgb(100, 100, 100))
        })
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(border_color));

    let inner = block.inner(area);
    block.render(area, buf);

    // Split for content and footer
    let layout = Layout::vertical([
        Constraint::Min(1),
        Constraint::Length(1),
    ])
    .split(inner);

    let editor_area = layout[0];
    let footer_area = layout[1];

    // Render the editor content with syntax highlighting
    let lines = app.sql_editor.lines();
    let cursor = app.sql_editor.cursor();

    let highlighted_lines: Vec<Line> = lines
        .iter()
        .enumerate()
        .map(|(line_idx, line)| {
            if line.is_empty() && !is_focused {
                // Show placeholder for empty editor
                if line_idx == 0 && lines.len() == 1 {
                    return Line::from(Span::styled(
                        "-- type : to focus · F5 or Shift+Enter to run",
                        Style::default().fg(Color::Rgb(80, 80, 80)).italic(),
                    ));
                }
            }
            highlight_sql_line(line, line_idx, cursor, is_focused)
        })
        .collect();

    let editor_widget = Paragraph::new(highlighted_lines)
        .style(Style::default().fg(Color::White));

    editor_widget.render(editor_area, buf);

    // Draw cursor if focused
    if is_focused {
        let (cursor_row, cursor_col) = cursor;
        let cursor_y = editor_area.y + cursor_row as u16;
        let cursor_x = editor_area.x + cursor_col as u16;

        if cursor_y < editor_area.y + editor_area.height
            && cursor_x < editor_area.x + editor_area.width
            && let Some(cell) = buf.cell_mut((cursor_x, cursor_y))
        {
            cell.set_style(Style::default().bg(Color::White).fg(Color::Black));
        }
    }

    // Footer with history indicator and running state
    let history_indicator = if let Some(idx) = app.history_index {
        format!("history [{}/{}]", idx + 1, app.query_history.len())
    } else {
        String::new()
    };

    let footer = if app.query_executing {
        // Show running indicator
        let elapsed = app.query_elapsed_ms().unwrap_or(0);
        Line::from(vec![
            Span::styled("⟳ Running", Style::default().fg(Color::Yellow).bold()),
            Span::styled(format!(" {}ms...", elapsed), Style::default().fg(Color::Yellow)),
        ])
    } else {
        Line::from(vec![
            Span::styled("F5", Style::default().fg(Color::Cyan)),
            Span::styled("/", Style::default().fg(Color::DarkGray)),
            Span::styled("Shift+Enter", Style::default().fg(Color::Cyan)),
            Span::styled(" run  ", Style::default().fg(Color::DarkGray)),
            Span::styled("↑↓", Style::default().fg(Color::Cyan)),
            Span::styled(" history", Style::default().fg(Color::DarkGray)),
            if !history_indicator.is_empty() {
                Span::styled(format!("  │ {}", history_indicator), Style::default().fg(Color::Rgb(80, 80, 80)))
            } else {
                Span::raw("")
            },
        ])
    };

    Paragraph::new(footer)
        .alignment(Alignment::Center)
        .render(footer_area, buf);
}

/// Highlight a single SQL line.
fn highlight_sql_line(line: &str, line_idx: usize, cursor: (usize, usize), is_focused: bool) -> Line<'static> {
    let (cursor_row, _cursor_col) = cursor;
    let is_cursor_line = line_idx == cursor_row && is_focused;

    let mut spans: Vec<Span<'static>> = Vec::new();
    let mut i = 0;
    let chars: Vec<char> = line.chars().collect();

    while i < chars.len() {
        // Check for string literals (single quotes)
        if chars[i] == '\'' {
            let start = i;
            i += 1;
            while i < chars.len() && chars[i] != '\'' {
                i += 1;
            }
            if i < chars.len() {
                i += 1; // Include closing quote
            }
            let s: String = chars[start..i].iter().collect();
            spans.push(Span::styled(s, Style::default().fg(Color::Green)));
            continue;
        }

        // Check for comments (--)
        if i + 1 < chars.len() && chars[i] == '-' && chars[i + 1] == '-' {
            let s: String = chars[i..].iter().collect();
            spans.push(Span::styled(s, Style::default().fg(Color::DarkGray).italic()));
            break;
        }

        // Check for keywords
        let remaining: String = chars[i..].iter().collect();
        let mut found_keyword = false;

        for &keyword in SQL_KEYWORDS {
            if remaining.to_uppercase().starts_with(keyword) {
                // Check it's a whole word
                let next_idx = i + keyword.len();
                let is_word_boundary = next_idx >= chars.len()
                    || !chars[next_idx].is_alphanumeric() && chars[next_idx] != '_';
                let is_start_boundary = i == 0
                    || !chars[i - 1].is_alphanumeric() && chars[i - 1] != '_';

                if is_word_boundary && is_start_boundary {
                    let s: String = chars[i..next_idx].iter().collect();
                    spans.push(Span::styled(s, Style::default().fg(Color::Blue).bold()));
                    i = next_idx;
                    found_keyword = true;
                    break;
                }
            }
        }

        if found_keyword {
            continue;
        }

        // Check for numbers
        if chars[i].is_ascii_digit() {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            let s: String = chars[start..i].iter().collect();
            spans.push(Span::styled(s, Style::default().fg(Color::Rgb(255, 180, 100))));
            continue;
        }

        // Regular character
        spans.push(Span::styled(
            chars[i].to_string(),
            Style::default().fg(Color::White),
        ));
        i += 1;
    }

    // Highlight cursor line background slightly
    let line_style = if is_cursor_line {
        Style::default().bg(Color::Rgb(30, 30, 40))
    } else {
        Style::default()
    };

    Line::from(spans).style(line_style)
}
