use ratatui::{
    buffer::Buffer,
    layout::{Alignment, Constraint, Layout, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, BorderType, Paragraph, Widget},
};

use crate::app::{App, ConnectionState};

impl Widget for &App {
    fn render(self, area: Rect, buf: &mut Buffer) {
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
        let status_line = match &self.connection {
            ConnectionState::Connecting => {
                Line::from(vec![
                    Span::styled("⟳ ", Style::default().fg(Color::Yellow)),
                    Span::styled("Connecting...", Style::default().fg(Color::Yellow)),
                ])
            }
            ConnectionState::Connected { db_name, .. } => {
                Line::from(vec![
                    Span::styled("● ", Style::default().fg(Color::Green)),
                    Span::styled(
                        format!("Connected to {db_name}"),
                        Style::default().fg(Color::Green),
                    ),
                ])
            }
            ConnectionState::Failed { error } => {
                Line::from(vec![
                    Span::styled("✗ ", Style::default().fg(Color::Red)),
                    Span::styled(
                        format!("Connection failed: {error}"),
                        Style::default().fg(Color::Red),
                    ),
                ])
            }
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
}
