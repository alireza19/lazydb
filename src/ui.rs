use ratatui::{
    buffer::Buffer,
    layout::{Alignment, Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, BorderType, Cell, Paragraph, Row, Table, Widget, Wrap},
};
use tui_logger::TuiLoggerSmartWidget;

use crate::app::{App, ConnectionState, CurrentView, FocusedPane, QueryResultState, TableViewState, TreeNodeId};
use crate::dotline::{make_color_fn, AsciiDotGraph};

const BORDER_NORMAL: Color = Color::White;
const BORDER_FOCUSED: Color = Color::Rgb(255, 140, 0);
const TEXT_NORMAL: Color = Color::White;
const TEXT_DIM: Color = Color::DarkGray;
const TEXT_SUCCESS: Color = Color::Green;
const TEXT_ERROR: Color = Color::Red;
const SELECTED_BG: Color = Color::Rgb(255, 140, 0);
const SELECTED_FG: Color = Color::Black;
const SEPARATOR: Color = Color::Rgb(80, 80, 80);
const ICON_GRAY: Color = Color::Rgb(180, 180, 180);
const PK_COLOR: Color = Color::Rgb(255, 200, 100);
const NUMBER_COLOR: Color = Color::Rgb(255, 180, 100);
const CURSOR_LINE_BG: Color = Color::Rgb(40, 40, 40);

const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN", "IS", "NULL",
    "ORDER", "BY", "ASC", "DESC", "LIMIT", "OFFSET", "GROUP", "HAVING", "JOIN", "LEFT",
    "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "ON", "AS", "DISTINCT", "COUNT", "SUM",
    "AVG", "MIN", "MAX", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "CREATE",
    "TABLE", "DROP", "ALTER", "INDEX", "VIEW", "TRIGGER", "FUNCTION", "PROCEDURE", "BEGIN",
    "END", "IF", "ELSE", "THEN", "CASE", "WHEN", "COALESCE", "NULLIF", "CAST",
    "UNION", "ALL", "INTERSECT", "EXCEPT", "EXISTS", "ANY", "SOME", "EXPLAIN", "ANALYZE",
    "WITH", "RECURSIVE", "RETURNING", "CONFLICT", "DO", "NOTHING", "TRUE", "FALSE",
];

fn title_style() -> Style {
    Style::default().fg(TEXT_NORMAL).add_modifier(Modifier::BOLD)
}

fn border_style(focused: bool) -> Style {
    Style::default().fg(if focused { BORDER_FOCUSED } else { BORDER_NORMAL })
}

fn pane_block(title: &str, focused: bool) -> Block<'_> {
    Block::bordered()
        .title(title)
        .title_style(title_style())
        .border_type(BorderType::Rounded)
        .border_style(border_style(focused))
}

impl Widget for &App {
    fn render(self, area: Rect, buf: &mut Buffer) {
        match &self.current_view {
            CurrentView::ConnectionStatus => render_connection_status(self, area, buf),
            _ => render_main_layout(self, area, buf),
        }
    }
}

fn render_connection_status(app: &App, area: Rect, buf: &mut Buffer) {
    let block = pane_block(" lazydb ", false).title_alignment(Alignment::Center);
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
            Span::styled("⟳ ", Style::default().fg(TEXT_NORMAL)),
            Span::styled("Connecting...", Style::default().fg(TEXT_NORMAL)),
        ]),
        ConnectionState::Connected { db_name, .. } => Line::from(vec![
            Span::styled("● ", Style::default().fg(TEXT_SUCCESS)),
            Span::styled(format!("Connected to {db_name}"), Style::default().fg(TEXT_SUCCESS)),
        ]),
        ConnectionState::Failed { error } => Line::from(vec![
            Span::styled("✗ ", Style::default().fg(TEXT_ERROR)),
            Span::styled(format!("Connection failed: {error}"), Style::default().fg(TEXT_ERROR)),
        ]),
    };

    Paragraph::new(status_line).alignment(Alignment::Center).render(layout[1], buf);

    Paragraph::new(Line::from(vec![
        Span::styled("Press ", Style::default().fg(TEXT_DIM)),
        Span::styled("q", Style::default().fg(TEXT_NORMAL).bold()),
        Span::styled(" to quit", Style::default().fg(TEXT_DIM)),
    ]))
        .alignment(Alignment::Center)
        .render(layout[2], buf);
}

fn render_main_layout(app: &App, area: Rect, buf: &mut Buffer) {
    let block = pane_block(" lazydb ", false).title_alignment(Alignment::Center);
    let inner = block.inner(area);
    block.render(area, buf);

    let outer = Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).split(inner);
    let main_vertical = Layout::vertical([Constraint::Percentage(70), Constraint::Percentage(30)])
        .split(outer[0]);
    let top_horizontal = Layout::horizontal([Constraint::Percentage(30), Constraint::Percentage(70)])
        .split(main_vertical[0]);
    let right_stack = Layout::vertical([Constraint::Percentage(70), Constraint::Percentage(30)])
        .split(top_horizontal[1]);
    let bottom_horizontal = Layout::horizontal([Constraint::Percentage(30), Constraint::Percentage(70)])
        .split(main_vertical[1]);

    render_sidebar(app, top_horizontal[0], buf);
    render_content_area(app, right_stack[0], buf);
    render_sql_editor(app, right_stack[1], buf);
    render_stats_panel(app, bottom_horizontal[0], buf);
    render_logs_panel(app, bottom_horizontal[1], buf);
    render_global_status_bar(app, outer[1], buf);
}

fn render_global_status_bar(app: &App, area: Rect, buf: &mut Buffer) {
    Paragraph::new(Line::from(vec![
        Span::styled(format!("[{}]", app.focused_pane.label()), Style::default().fg(BORDER_FOCUSED).bold()),
        Span::styled(" │ ", Style::default().fg(SEPARATOR)),
        Span::styled("Tab", Style::default().fg(TEXT_NORMAL)),
        Span::styled(" cycle  ", Style::default().fg(TEXT_DIM)),
        Span::styled(":", Style::default().fg(TEXT_NORMAL)),
        Span::styled(" SQL  ", Style::default().fg(TEXT_DIM)),
        Span::styled("q", Style::default().fg(TEXT_NORMAL)),
        Span::styled(" quit", Style::default().fg(TEXT_DIM)),
    ]))
        .alignment(Alignment::Center)
        .render(area, buf);
}

fn render_sidebar(app: &App, area: Rect, buf: &mut Buffer) {
    let db_name = match &app.connection {
        ConnectionState::Connected { db_name, .. } => db_name.as_str(),
        _ => "database",
    };

    let title = format!(" {} ", db_name);
    let block = pane_block(&title, app.focused_pane == FocusedPane::Sidebar);
    let inner = block.inner(area);
    block.render(area, buf);

    let layout = Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).split(inner);
    let (lines, selected_idx) = build_tree_lines(app);

    if lines.is_empty() {
        let centered = Layout::vertical([Constraint::Fill(1), Constraint::Length(1), Constraint::Fill(1)])
            .split(layout[0]);
        Paragraph::new(Span::styled("Loading...", Style::default().fg(TEXT_DIM).italic()))
            .alignment(Alignment::Center)
            .render(centered[1], buf);
    } else {
        let visible_height = layout[0].height as usize;
        let scroll_offset = selected_idx.map_or(0, |idx| {
            if idx < visible_height / 2 {
                0
            } else if idx >= lines.len().saturating_sub(visible_height / 2) {
                lines.len().saturating_sub(visible_height)
            } else {
                idx.saturating_sub(visible_height / 2)
            }
        });

        let visible_lines: Vec<Line> = lines
            .into_iter()
            .skip(scroll_offset)
            .take(visible_height)
            .collect();
        Paragraph::new(visible_lines).render(layout[0], buf);
    }

    let table_count: usize = app.db_structure.as_ref().map_or(0, |s| s.schemas.iter().map(|sc| sc.tables.len()).sum());
    Paragraph::new(Span::styled(
        format!("{} tables │ r refresh │ ←→ expand", table_count),
        Style::default().fg(TEXT_DIM),
    ))
    .alignment(Alignment::Center)
    .render(layout[1], buf);
}

fn build_tree_lines(app: &App) -> (Vec<Line<'static>>, Option<usize>) {
    let Some(structure) = &app.db_structure else {
        return (vec![], None);
    };

    let selected = app.tree_state.selected();
    let opened = app.tree_state.opened();
    let selected_table = app.selected_table.as_ref();

    let db_name = match &app.connection {
        ConnectionState::Connected { db_name, .. } => db_name.clone(),
        _ => "database".to_string(),
    };

    let mut lines = Vec::new();
    let mut selected_idx = None;

    let root_path = vec![TreeNodeId::Root];
    let root_expanded = opened.iter().any(|p| p == &root_path);
    if selected == root_path {
        selected_idx = Some(lines.len());
    }
    lines.push(tree_line(0, root_expanded, true, selected == root_path, vec![
        Span::styled("󰆼 ", Style::default().fg(TEXT_SUCCESS)),
        Span::styled(db_name, Style::default().fg(TEXT_NORMAL).bold()),
    ]));

    if !root_expanded {
        return (lines, selected_idx);
    }

    for schema in &structure.schemas {
        let schema_path = vec![TreeNodeId::Root, TreeNodeId::Schema(schema.name.clone())];
        let schema_expanded = opened.iter().any(|p| p == &schema_path);
        let is_selected = selected == schema_path;
        if is_selected {
            selected_idx = Some(lines.len());
        }

        let icon = if schema.name == "public" { "󰉖 " } else { "󰉋 " };
        lines.push(tree_line(1, schema_expanded, !schema.tables.is_empty(), is_selected, vec![
            Span::styled(icon, Style::default().fg(ICON_GRAY)),
            Span::styled(schema.name.clone(), Style::default().fg(TEXT_NORMAL)),
            Span::styled(format!(" ({})", schema.tables.len()), Style::default().fg(TEXT_DIM)),
        ]));

        if !schema_expanded {
            continue;
        }

        for table in &schema.tables {
            let table_path = vec![
                TreeNodeId::Root,
                TreeNodeId::Schema(schema.name.clone()),
                TreeNodeId::Table { schema: schema.name.clone(), table: table.name.clone() },
            ];
            let table_expanded = opened.iter().any(|p| p == &table_path);
            let is_selected = selected == table_path;
            if is_selected {
                selected_idx = Some(lines.len());
            }

            let is_viewing = selected_table.is_some_and(|(s, t)| s == &schema.name && t == &table.name);
            let style = if is_viewing {
                Style::default().fg(TEXT_SUCCESS).bold()
    } else {
                Style::default().fg(TEXT_NORMAL)
            };

            lines.push(tree_line(2, table_expanded, !table.columns.is_empty(), is_selected, vec![
                Span::styled("󰓫 ", style),
                Span::styled(table.name.clone(), style),
            ]));

            if !table_expanded {
                continue;
            }

            for col in &table.columns {
                let col_path = vec![
                    TreeNodeId::Root,
                    TreeNodeId::Schema(schema.name.clone()),
                    TreeNodeId::Table { schema: schema.name.clone(), table: table.name.clone() },
                    TreeNodeId::Column { schema: schema.name.clone(), table: table.name.clone(), column: col.name.clone() },
                ];
                let is_selected = selected == col_path;
                if is_selected {
                    selected_idx = Some(lines.len());
                }

                let icon = if col.is_primary_key {
                    Span::styled("󰌋 ", Style::default().fg(PK_COLOR))
                } else if col.is_nullable {
                    Span::styled("○ ", Style::default().fg(TEXT_DIM))
                } else {
                    Span::styled("• ", Style::default().fg(TEXT_NORMAL))
                };

                lines.push(tree_line(3, false, false, is_selected, vec![
                    icon,
                    Span::styled(col.name.clone(), Style::default().fg(TEXT_NORMAL)),
                    Span::raw(" "),
                    Span::styled(col.data_type.clone(), Style::default().fg(TEXT_DIM)),
                ]));
            }
        }
    }

    (lines, selected_idx)
}

fn tree_line(depth: usize, expanded: bool, has_children: bool, selected: bool, content: Vec<Span<'static>>) -> Line<'static> {
    let arrow = match (has_children, expanded) {
        (true, true) => "▾ ",
        (true, false) => "▸ ",
        (false, _) => "  ",
    };

    let mut spans = vec![
        Span::raw("  ".repeat(depth)),
        Span::styled(arrow, Style::default().fg(TEXT_DIM)),
    ];
    spans.extend(content);

    let line = Line::from(spans);
    if selected {
        line.style(Style::default().fg(SELECTED_FG).bg(SELECTED_BG))
    } else {
        line
    }
}

fn render_stats_panel(app: &App, area: Rect, buf: &mut Buffer) {
    let block = pane_block(" ◉ Live Monitor ", app.focused_pane == FocusedPane::Stats);
    let inner = block.inner(area);
    block.render(area, buf);

    let layout = Layout::vertical([Constraint::Min(4), Constraint::Length(3)]).split(inner);
    render_ascii_graphs(app, layout[0], buf);
    render_stats_info(app, layout[1], buf);
}

fn render_ascii_graphs(app: &App, area: Rect, buf: &mut Buffer) {
    let rows = Layout::vertical([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)]).split(area);
    let top_cols = Layout::horizontal([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)]).split(rows[0]);
    let bottom_cols = Layout::horizontal([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)]).split(rows[1]);

    render_graph("qps", &app.stats.queries_per_sec, &make_color_fn(50, true), top_cols[0], buf);
    render_graph("rows", &app.stats.rows_per_sec, &make_color_fn(10000, true), top_cols[1], buf);
    render_graph("ms", &app.stats.latency_ms, &make_color_fn(300, false), bottom_cols[0], buf);
    render_graph("conn", &app.stats.connections, &make_color_fn(20, true), bottom_cols[1], buf);
}

fn render_graph<F: Fn(u64, u64) -> Color>(
    label: &str,
    data: &std::collections::VecDeque<u64>,
    color_fn: &F,
    area: Rect,
    buf: &mut Buffer,
) {
    if area.height == 0 || area.width == 0 {
        return;
    }

    let layout = Layout::vertical([Constraint::Length(1), Constraint::Min(1)]).split(area);
    let current = data.back().copied().unwrap_or(0);
    let max = data.iter().max().copied().unwrap_or(1).max(1);

    Paragraph::new(Line::from(vec![
        Span::styled(format!(" {} ", label), Style::default().fg(TEXT_DIM)),
        Span::styled(format!("{:>4}", current), Style::default().fg(color_fn(current, max)).bold()),
    ]))
    .render(layout[0], buf);

    AsciiDotGraph::new(data, max, color_fn)
        .height(layout[1].height.max(1))
        .render(layout[1], buf);
}

fn render_stats_info(app: &App, area: Rect, buf: &mut Buffer) {
    Paragraph::new(vec![
        Line::from(vec![
            Span::styled("● ", Style::default().fg(TEXT_SUCCESS)),
            Span::styled(&app.stats.host, Style::default().fg(TEXT_NORMAL)),
            Span::styled(" │ ", Style::default().fg(SEPARATOR)),
            Span::styled(&app.stats.database, Style::default().fg(TEXT_NORMAL).bold()),
        ]),
        Line::from(vec![
            Span::styled("Tables: ", Style::default().fg(TEXT_DIM)),
            Span::styled(format!("{}", app.stats.table_count), Style::default().fg(TEXT_NORMAL)),
            Span::styled(" │ ", Style::default().fg(SEPARATOR)),
            Span::styled("Last: ", Style::default().fg(TEXT_DIM)),
            Span::styled(
                app.stats.last_query_ms.map_or("—".into(), |ms| format!("{}ms", ms)),
                latency_style(app.stats.last_query_ms.unwrap_or(0) as u64),
            ),
            Span::styled(" │ ", Style::default().fg(SEPARATOR)),
            Span::styled("Total: ", Style::default().fg(TEXT_DIM)),
            Span::styled(format!("{}", app.stats.queries_run), Style::default().fg(TEXT_NORMAL)),
        ]),
        Line::from(Span::styled(
            if app.stats.pg_version.is_empty() { "PostgreSQL" } else { &app.stats.pg_version },
            Style::default().fg(TEXT_DIM).italic(),
        )),
    ])
    .render(area, buf);
}

fn render_logs_panel(app: &App, area: Rect, buf: &mut Buffer) {
    let block = pane_block(" 󰌱 DB Logs ", app.focused_pane == FocusedPane::Logs);
    let inner = block.inner(area);
    block.render(area, buf);

    TuiLoggerSmartWidget::default()
        .style_error(Style::default().fg(TEXT_ERROR))
        .style_warn(Style::default().fg(BORDER_FOCUSED))
        .style_info(Style::default().fg(TEXT_NORMAL))
        .style_debug(Style::default().fg(TEXT_SUCCESS))
        .style_trace(Style::default().fg(TEXT_DIM))
        .state(&app.logs_state)
        .render(inner, buf);
}

fn latency_style(ms: u64) -> Style {
    Style::default().fg(match ms {
        0 => TEXT_DIM,
        1..100 => Color::Rgb(80, 255, 80),
        100..200 => Color::Rgb(255, 255, 0),
        200..300 => Color::Rgb(255, 165, 0),
        _ => Color::Rgb(255, 80, 80),
    })
}

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

fn render_placeholder(app: &App, area: Rect, buf: &mut Buffer) {
    let block = pane_block(" Results ", app.focused_pane == FocusedPane::Results);
    let inner = block.inner(area);
    block.render(area, buf);

    let centered = Layout::vertical([
        Constraint::Fill(1),
        Constraint::Length(1),
        Constraint::Length(2),
        Constraint::Fill(1),
    ])
    .split(inner);

    Paragraph::new(Line::from(vec![
        Span::styled("Select a table ", Style::default().fg(TEXT_DIM)),
        Span::styled("→", Style::default().fg(TEXT_NORMAL)),
    ]))
    .alignment(Alignment::Center)
    .render(centered[1], buf);

    Paragraph::new(Line::from(vec![
        Span::styled("↑↓", Style::default().fg(TEXT_NORMAL)),
        Span::styled(" navigate  ", Style::default().fg(TEXT_DIM)),
        Span::styled("Enter", Style::default().fg(TEXT_NORMAL)),
        Span::styled(" select  ", Style::default().fg(TEXT_DIM)),
        Span::styled(":", Style::default().fg(TEXT_NORMAL)),
        Span::styled(" SQL  ", Style::default().fg(TEXT_DIM)),
        Span::styled("q", Style::default().fg(TEXT_NORMAL)),
        Span::styled(" quit", Style::default().fg(TEXT_DIM)),
    ]))
        .alignment(Alignment::Center)
    .render(centered[2], buf);
}

fn render_table_view(state: &TableViewState, app: &App, area: Rect, buf: &mut Buffer) {
    let title = format!(" {} ", state.table_name);
    let block = pane_block(&title, app.focused_pane == FocusedPane::Results);
    let inner = block.inner(area);
    block.render(area, buf);

    let layout = Layout::vertical([Constraint::Min(3), Constraint::Length(1)]).split(inner);

    if state.loading {
        render_centered_message(layout[0], buf, "⟳ ", "Loading...", TEXT_NORMAL);
    } else if let Some(error) = &state.error {
        render_centered_message(layout[0], buf, "✗ ", error, TEXT_ERROR);
    } else if state.rows.is_empty() {
        render_centered_message(layout[0], buf, "", "<empty table>", TEXT_DIM);
    } else {
        render_data_table(&state.columns, &state.rows, state.selected_row, state.scroll_offset, layout[0], buf);
    }

    render_table_footer(state, layout[1], buf);
}

fn render_query_results(qr: &QueryResultState, app: &App, area: Rect, buf: &mut Buffer) {
    let title = if qr.error.is_some() { " Query Error " } else { " Query Results " };
    let block = pane_block(title, app.focused_pane == FocusedPane::Results);
    let inner = block.inner(area);
    block.render(area, buf);

    let layout = Layout::vertical([Constraint::Min(3), Constraint::Length(1)]).split(inner);

    if let Some(error) = &qr.error {
        Paragraph::new(error.clone())
            .style(Style::default().fg(TEXT_ERROR))
            .wrap(Wrap { trim: false })
            .render(layout[0], buf);
    } else if qr.rows.is_empty() {
        if qr.columns.is_empty() {
            render_centered_message(layout[0], buf, "✓ ", "Query executed successfully", TEXT_SUCCESS);
        } else {
            render_centered_message(layout[0], buf, "", "<no rows returned>", TEXT_DIM);
        }
    } else if qr.is_explain {
        let lines: Vec<Line> = qr.rows.iter()
            .map(|row| Line::from(Span::styled(row.first().map(|s| s.as_str()).unwrap_or(""), Style::default().fg(TEXT_NORMAL))))
            .collect();
        Paragraph::new(lines).wrap(Wrap { trim: false }).render(layout[0], buf);
    } else {
        render_data_table(&qr.columns, &qr.rows, qr.selected_row, qr.scroll_offset, layout[0], buf);
    }

    Paragraph::new(Line::from(vec![
        Span::styled(format!("{} rows", qr.row_count), Style::default().fg(TEXT_NORMAL)),
        Span::styled(" │ ", Style::default().fg(SEPARATOR)),
        Span::styled(format!("{}ms", qr.duration_ms), Style::default().fg(TEXT_SUCCESS)),
        Span::styled(" │ ", Style::default().fg(SEPARATOR)),
        Span::styled("c", Style::default().fg(TEXT_NORMAL)),
        Span::styled(" clear", Style::default().fg(TEXT_DIM)),
    ]))
        .alignment(Alignment::Center)
    .render(layout[1], buf);
}

fn render_centered_message(area: Rect, buf: &mut Buffer, prefix: &str, msg: &str, color: Color) {
    let centered = Layout::vertical([Constraint::Fill(1), Constraint::Length(1), Constraint::Fill(1)]).split(area);
    Paragraph::new(Line::from(vec![
        Span::styled(prefix, Style::default().fg(color)),
        Span::styled(msg, Style::default().fg(color)),
    ]))
        .alignment(Alignment::Center)
        .render(centered[1], buf);
}

fn render_data_table(columns: &[String], rows: &[Vec<String>], selected_row: usize, scroll_offset: usize, area: Rect, buf: &mut Buffer) {
    if columns.is_empty() {
        return;
    }

    let mut col_widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < col_widths.len() {
                col_widths[i] = col_widths[i].max(cell.len().min(30));
            }
        }
    }

    let constraints: Vec<Constraint> = col_widths.iter().map(|&w| Constraint::Length((w + 2) as u16)).collect();
    let header = Row::new(columns.iter().map(|col| Cell::from(col.clone()).style(Style::default().fg(TEXT_NORMAL).bold()))).height(1);

    let visible_rows = area.height.saturating_sub(1) as usize;
    let end_idx = (scroll_offset + visible_rows).min(rows.len());

    let data_rows: Vec<Row> = rows[scroll_offset..end_idx]
        .iter()
        .enumerate()
        .map(|(visible_idx, row)| {
            let is_selected = scroll_offset + visible_idx == selected_row;
            let row_style = if is_selected {
                Style::default().bg(SELECTED_BG).fg(SELECTED_FG)
                    } else {
                Style::default()
            };

            let cells: Vec<Cell> = row.iter().map(|cell| {
                let display = if cell.len() > 30 { format!("{}…", &cell[..29]) } else { cell.clone() };
                let style = if is_selected {
                    Style::default().fg(SELECTED_FG).bg(SELECTED_BG)
                } else if cell == "NULL" {
                    Style::default().fg(TEXT_DIM).italic()
                    } else {
                    Style::default().fg(TEXT_NORMAL)
                    };
                    Cell::from(display).style(style)
            }).collect();

            Row::new(cells).style(row_style).height(1)
        })
        .collect();

    Table::new(data_rows, constraints).header(header).render(area, buf);
}

fn render_table_footer(state: &TableViewState, area: Rect, buf: &mut Buffer) {
    Paragraph::new(Line::from(vec![
        Span::styled("Page ", Style::default().fg(TEXT_DIM)),
        Span::styled(format!("{}", state.page + 1), Style::default().fg(TEXT_NORMAL)),
        Span::styled(format!("/{}", state.total_pages()), Style::default().fg(TEXT_DIM)),
        Span::styled(" │ ", Style::default().fg(SEPARATOR)),
        Span::styled("Rows: ", Style::default().fg(TEXT_DIM)),
        Span::styled(format!("~{}", state.total_count), Style::default().fg(TEXT_NORMAL)),
        Span::styled(" │ ", Style::default().fg(SEPARATOR)),
        Span::styled("←→", Style::default().fg(TEXT_NORMAL)),
        Span::styled(" page  ", Style::default().fg(TEXT_DIM)),
        Span::styled("↑↓", Style::default().fg(TEXT_NORMAL)),
        Span::styled(" row", Style::default().fg(TEXT_DIM)),
    ]))
        .alignment(Alignment::Center)
        .render(area, buf);
}

fn render_sql_editor(app: &App, area: Rect, buf: &mut Buffer) {
    let is_focused = app.focused_pane == FocusedPane::Editor;

    let title = if app.query_executing {
        format!(" SQL ⟳ {}ms ", app.query_elapsed_ms().unwrap_or(0))
    } else if is_focused {
        " SQL [editing] ".to_string()
    } else {
        " SQL ".to_string()
    };

    let block = Block::bordered()
        .title(title)
        .title_style(if is_focused { Style::default().fg(BORDER_FOCUSED).bold() } else { Style::default().fg(TEXT_DIM) })
        .border_type(BorderType::Rounded)
        .border_style(border_style(is_focused));

    let inner = block.inner(area);
    block.render(area, buf);

    let layout = Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).split(inner);
    let editor_area = layout[0];

    let lines = app.sql_editor.lines();
    let cursor = app.sql_editor.cursor();
    let visible_rows = editor_area.height as usize;
    let scroll_offset = app.editor_scroll_offset.min(lines.len().saturating_sub(1));
    let end_idx = (scroll_offset + visible_rows).min(lines.len());

    let highlighted_lines: Vec<Line> = lines.iter().enumerate()
        .skip(scroll_offset)
        .take(end_idx - scroll_offset)
        .map(|(line_idx, line)| {
            if line.is_empty() && !is_focused && line_idx == 0 && lines.len() == 1 {
                    return Line::from(Span::styled(
                        "-- type : to focus · F5 or Shift+Enter to run",
                    Style::default().fg(TEXT_DIM).italic(),
                    ));
            }
            highlight_sql_line(line, line_idx, cursor, is_focused)
        })
        .collect();

    Paragraph::new(highlighted_lines).render(editor_area, buf);

    if is_focused && cursor.0 >= scroll_offset && cursor.0 < end_idx {
        let cursor_y = editor_area.y + (cursor.0 - scroll_offset) as u16;
        let cursor_x = editor_area.x + cursor.1 as u16;
        if cursor_y < editor_area.y + editor_area.height
            && cursor_x < editor_area.x + editor_area.width
            && let Some(cell) = buf.cell_mut((cursor_x, cursor_y))
        {
            cell.set_style(Style::default().bg(Color::White).fg(Color::Black));
        }
    }

    let total_lines = lines.len();
    if total_lines > visible_rows && visible_rows > 0 {
        let scrollbar_height = editor_area.height.saturating_sub(1).max(1);
        let scroll_ratio = scroll_offset as f32 / (total_lines - visible_rows).max(1) as f32;
        let thumb_pos = (scroll_ratio * (scrollbar_height - 1) as f32) as u16;
        let scroll_x = editor_area.x + editor_area.width - 1;

        for y in 0..editor_area.height {
            if let Some(cell) = buf.cell_mut((scroll_x, editor_area.y + y)) {
                if y == thumb_pos {
                    cell.set_char('█');
                    cell.set_style(Style::default().fg(Color::Rgb(120, 120, 120)));
    } else {
                    cell.set_char('│');
                    cell.set_style(Style::default().fg(Color::Rgb(60, 60, 60)));
                }
            }
        }
    }

    let footer = if app.query_executing {
        Line::from(vec![
            Span::styled("⟳ Running", Style::default().fg(BORDER_FOCUSED).bold()),
            Span::styled(format!(" {}ms...", app.query_elapsed_ms().unwrap_or(0)), Style::default().fg(BORDER_FOCUSED)),
        ])
    } else {
        let mut spans = vec![
            Span::styled("F5", Style::default().fg(TEXT_NORMAL)),
            Span::styled("/", Style::default().fg(TEXT_DIM)),
            Span::styled("Shift+Enter", Style::default().fg(TEXT_NORMAL)),
            Span::styled(" run  ", Style::default().fg(TEXT_DIM)),
            Span::styled("↑↓", Style::default().fg(TEXT_NORMAL)),
            Span::styled(" history", Style::default().fg(TEXT_DIM)),
        ];
        if let Some(idx) = app.history_index {
            spans.push(Span::styled(
                format!("  │ history [{}/{}]", idx + 1, app.query_history.len()),
                Style::default().fg(TEXT_DIM),
            ));
        }
        Line::from(spans)
    };

    Paragraph::new(footer).alignment(Alignment::Center).render(layout[1], buf);
}

fn highlight_sql_line(line: &str, line_idx: usize, cursor: (usize, usize), is_focused: bool) -> Line<'static> {
    let is_cursor_line = line_idx == cursor.0 && is_focused;
    let mut spans: Vec<Span<'static>> = Vec::new();
    let chars: Vec<char> = line.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        if chars[i] == '\'' {
            let start = i;
            i += 1;
            while i < chars.len() && chars[i] != '\'' {
                i += 1;
            }
            if i < chars.len() {
                i += 1;
            }
            spans.push(Span::styled(chars[start..i].iter().collect::<String>(), Style::default().fg(TEXT_SUCCESS)));
            continue;
        }

        if i + 1 < chars.len() && chars[i] == '-' && chars[i + 1] == '-' {
            spans.push(Span::styled(chars[i..].iter().collect::<String>(), Style::default().fg(TEXT_DIM).italic()));
            break;
        }

        let remaining: String = chars[i..].iter().collect();
        let mut found_keyword = false;
        for &keyword in SQL_KEYWORDS {
            if remaining.to_uppercase().starts_with(keyword) {
                let next_idx = i + keyword.len();
                let is_word_boundary = next_idx >= chars.len() || (!chars[next_idx].is_alphanumeric() && chars[next_idx] != '_');
                let is_start_boundary = i == 0 || (!chars[i - 1].is_alphanumeric() && chars[i - 1] != '_');
                if is_word_boundary && is_start_boundary {
                    spans.push(Span::styled(chars[i..next_idx].iter().collect::<String>(), Style::default().fg(TEXT_NORMAL).bold()));
                    i = next_idx;
                    found_keyword = true;
                    break;
                }
            }
        }
        if found_keyword {
            continue;
        }

        if chars[i].is_ascii_digit() {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            spans.push(Span::styled(chars[start..i].iter().collect::<String>(), Style::default().fg(NUMBER_COLOR)));
            continue;
        }

        spans.push(Span::styled(chars[i].to_string(), Style::default().fg(TEXT_NORMAL)));
        i += 1;
    }

    let line_style = if is_cursor_line { Style::default().bg(CURSOR_LINE_BG) } else { Style::default() };
    Line::from(spans).style(line_style)
}
