use ratatui::{
    buffer::Buffer,
    layout::{Alignment, Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{
        Block, BorderType, Cell, List, ListItem, ListState, Paragraph, Row, Table, Widget,
    },
};

use crate::app::{App, ConnectionState, CurrentView, TableViewState};

impl Widget for &App {
    fn render(self, area: Rect, buf: &mut Buffer) {
        match &self.current_view {
            CurrentView::ConnectionStatus => render_connection_status(self, area, buf),
            CurrentView::TableList => render_main_view(self, area, buf),
            CurrentView::TableView(_) => render_main_view(self, area, buf),
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

/// Render the main view with sidebar and content area.
fn render_main_view(app: &App, area: Rect, buf: &mut Buffer) {
    let main_block = Block::bordered()
        .title(" lazydb ")
        .title_alignment(Alignment::Center)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = main_block.inner(area);
    main_block.render(area, buf);

    let horizontal_layout = Layout::horizontal([
        Constraint::Percentage(30),
        Constraint::Percentage(70),
    ])
    .split(inner);

    render_sidebar(app, horizontal_layout[0], buf);
    render_content_area(app, horizontal_layout[1], buf);
}

/// Render the left sidebar with table list.
fn render_sidebar(app: &App, area: Rect, buf: &mut Buffer) {
    let db_name = match &app.connection {
        ConnectionState::Connected { db_name, .. } => db_name.as_str(),
        _ => "database",
    };

    // Check if we're viewing a table (to highlight it differently)
    let viewing_table = match &app.current_view {
        CurrentView::TableView(state) => Some(state.table_name.as_str()),
        _ => None,
    };

    let sidebar_block = Block::bordered()
        .title(format!(" {} ", db_name))
        .title_style(Style::default().fg(Color::Cyan).bold())
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Rgb(60, 60, 60)));

    let sidebar_inner = sidebar_block.inner(area);
    sidebar_block.render(area, buf);

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
        .split(sidebar_inner);

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

        ratatui::widgets::StatefulWidget::render(list, sidebar_inner, buf, &mut state);
    }
}

/// Render the right content area.
fn render_content_area(app: &App, area: Rect, buf: &mut Buffer) {
    match &app.current_view {
        CurrentView::TableView(state) => render_table_view(state, area, buf),
        _ => render_placeholder(area, buf),
    }
}

/// Render placeholder when no table is selected.
fn render_placeholder(area: Rect, buf: &mut Buffer) {
    let content_block = Block::bordered()
        .title(" Table Details ")
        .title_style(Style::default().fg(Color::Magenta).bold())
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Rgb(60, 60, 60)));

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
        Span::styled("q", Style::default().fg(Color::Cyan)),
        Span::styled(" quit", Style::default().fg(Color::DarkGray)),
    ]);

    Paragraph::new(help_line)
        .alignment(Alignment::Center)
        .render(centered_layout[2], buf);
}

/// Render the table data view.
fn render_table_view(state: &TableViewState, area: Rect, buf: &mut Buffer) {
    let content_block = Block::bordered()
        .title(format!(" {} ", state.table_name))
        .title_style(Style::default().fg(Color::Magenta).bold())
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Rgb(60, 60, 60)));

    let content_inner = content_block.inner(area);
    content_block.render(area, buf);

    // Split into table area and footer
    let layout = Layout::vertical([
        Constraint::Min(3),
        Constraint::Length(1),
    ])
    .split(content_inner);

    let table_area = layout[0];
    let footer_area = layout[1];

    // Handle loading state
    if state.loading {
        let loading = Paragraph::new(Line::from(vec![
            Span::styled("⟳ ", Style::default().fg(Color::Yellow)),
            Span::styled("Loading...", Style::default().fg(Color::Yellow)),
        ]))
        .alignment(Alignment::Center);

        let centered = Layout::vertical([
            Constraint::Fill(1),
            Constraint::Length(1),
            Constraint::Fill(1),
        ])
        .split(table_area);

        loading.render(centered[1], buf);
    } else if let Some(error) = &state.error {
        // Handle error state
        let error_msg = Paragraph::new(Line::from(vec![
            Span::styled("✗ ", Style::default().fg(Color::Red)),
            Span::styled(error.clone(), Style::default().fg(Color::Red)),
        ]))
        .alignment(Alignment::Center);

        let centered = Layout::vertical([
            Constraint::Fill(1),
            Constraint::Length(1),
            Constraint::Fill(1),
        ])
        .split(table_area);

        error_msg.render(centered[1], buf);
    } else if state.rows.is_empty() {
        // Handle empty table
        let empty = Paragraph::new(Line::from(vec![Span::styled(
            "<empty table>",
            Style::default().fg(Color::DarkGray).italic(),
        )]))
        .alignment(Alignment::Center);

        let centered = Layout::vertical([
            Constraint::Fill(1),
            Constraint::Length(1),
            Constraint::Fill(1),
        ])
        .split(table_area);

        empty.render(centered[1], buf);
    } else {
        // Render the data table
        render_data_table(state, table_area, buf);
    }

    // Render footer
    render_table_footer(state, footer_area, buf);
}

/// Render the actual data table.
fn render_data_table(state: &TableViewState, area: Rect, buf: &mut Buffer) {
    // Calculate column widths based on content
    let col_count = state.columns.len();
    if col_count == 0 {
        return;
    }

    // Calculate max width for each column
    let mut col_widths: Vec<usize> = state.columns.iter().map(|c| c.len()).collect();
    for row in &state.rows {
        for (i, cell) in row.iter().enumerate() {
            if i < col_widths.len() {
                col_widths[i] = col_widths[i].max(cell.len().min(30)); // Cap at 30 chars
            }
        }
    }

    // Create constraints
    let constraints: Vec<Constraint> = col_widths
        .iter()
        .map(|&w| Constraint::Length((w + 2) as u16))
        .collect();

    // Create header row
    let header_cells: Vec<Cell> = state
        .columns
        .iter()
        .map(|col| {
            Cell::from(col.clone())
                .style(Style::default().fg(Color::Cyan).bold())
        })
        .collect();
    let header = Row::new(header_cells)
        .style(Style::default())
        .height(1);

    // Create data rows
    let rows: Vec<Row> = state
        .rows
        .iter()
        .enumerate()
        .map(|(i, row)| {
            let is_selected = i == state.selected_row;
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

    let table = Table::new(rows, constraints)
        .header(header)
        .row_highlight_style(Style::default().bg(Color::Rgb(40, 40, 60)));

    Widget::render(table, area, buf);
}

/// Render the table view footer.
fn render_table_footer(state: &TableViewState, area: Rect, buf: &mut Buffer) {
    let total_pages = state.total_pages();
    let current_page = state.page + 1;

    let footer = Line::from(vec![
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
        Span::styled(" row  ", Style::default().fg(Color::DarkGray)),
        Span::styled("b", Style::default().fg(Color::Cyan)),
        Span::styled("/", Style::default().fg(Color::DarkGray)),
        Span::styled("Esc", Style::default().fg(Color::Cyan)),
        Span::styled(" back  ", Style::default().fg(Color::DarkGray)),
        Span::styled("q", Style::default().fg(Color::Cyan)),
        Span::styled(" quit", Style::default().fg(Color::DarkGray)),
    ]);

    Paragraph::new(footer)
        .alignment(Alignment::Center)
        .render(area, buf);
}
