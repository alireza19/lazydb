use ratatui::{
    buffer::Buffer,
    layout::{Alignment, Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, BorderType, List, ListItem, ListState, Paragraph, Widget},
};

use crate::app::{App, ConnectionState, CurrentView};

impl Widget for &App {
    fn render(self, area: Rect, buf: &mut Buffer) {
        match self.current_view {
            CurrentView::ConnectionStatus => render_connection_status(self, area, buf),
            CurrentView::TableList => render_table_list(self, area, buf),
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

    // Connection status line
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

    // Help line
    let help_line = Line::from(vec![
        Span::styled("Press ", Style::default().fg(Color::DarkGray)),
        Span::styled("q", Style::default().fg(Color::White).bold()),
        Span::styled(" to quit", Style::default().fg(Color::DarkGray)),
    ]);

    Paragraph::new(help_line)
        .alignment(Alignment::Center)
        .render(layout[2], buf);
}

/// Render the table list view with sidebar.
fn render_table_list(app: &App, area: Rect, buf: &mut Buffer) {
    // Main container block
    let main_block = Block::bordered()
        .title(" lazydb ")
        .title_alignment(Alignment::Center)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = main_block.inner(area);
    main_block.render(area, buf);

    // Split into left sidebar (30%) and right content (70%)
    let horizontal_layout = Layout::horizontal([
        Constraint::Percentage(30),
        Constraint::Percentage(70),
    ])
    .split(inner);

    // Render left sidebar with table list
    render_sidebar(app, horizontal_layout[0], buf);

    // Render right content area
    render_content_area(app, horizontal_layout[1], buf);
}

/// Render the left sidebar with table list.
fn render_sidebar(app: &App, area: Rect, buf: &mut Buffer) {
    // Get db name for title
    let db_name = match &app.connection {
        ConnectionState::Connected { db_name, .. } => db_name.as_str(),
        _ => "database",
    };

    let sidebar_block = Block::bordered()
        .title(format!(" {} ", db_name))
        .title_style(Style::default().fg(Color::Cyan).bold())
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Rgb(60, 60, 60)));

    let sidebar_inner = sidebar_block.inner(area);
    sidebar_block.render(area, buf);

    if app.tables.is_empty() {
        // Show empty state
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
        // Build list items
        let items: Vec<ListItem> = app
            .tables
            .iter()
            .enumerate()
            .map(|(i, table)| {
                let is_selected = i == app.selected_table_index;
                let prefix = if is_selected { "▸ " } else { "  " };
                let style = if is_selected {
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

        // Create list state for highlighting
        let mut state = ListState::default();
        state.select(Some(app.selected_table_index));

        // Render list with state
        ratatui::widgets::StatefulWidget::render(list, sidebar_inner, buf, &mut state);
    }
}

/// Render the right content area.
fn render_content_area(_app: &App, area: Rect, buf: &mut Buffer) {
    let content_block = Block::bordered()
        .title(" Table Details ")
        .title_style(Style::default().fg(Color::Magenta).bold())
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Rgb(60, 60, 60)));

    let content_inner = content_block.inner(area);
    content_block.render(area, buf);

    // Center the placeholder text
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

    // Help line
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
