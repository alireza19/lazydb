//! ASCII bar-graph widget (gitui/btop style).
//!
//! Renders a fixed-height bar graph where each column is a solid vertical
//! bar using a single consistent glyph.

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Paragraph, Widget},
};
use std::collections::VecDeque;

/// Glyphs for graph segments (matches gitui style).
const DOT_SINGLE: char = '.';
const DOT_DOUBLE: char = ':';

/// Default graph height in rows.
pub const DEFAULT_HEIGHT: u16 = 4;

/// Dim color for zero/empty values.
const ZERO_COLOR: Color = Color::Rgb(50, 50, 50);

/// ASCII bar-graph widget with solid vertical bars.
///
/// Renders a fixed-height graph where each data point shows a solid
/// vertical bar from the peak position down to the baseline, using
/// a single consistent glyph throughout.
pub struct AsciiDotGraph<'a, F>
where
    F: Fn(u64, u64) -> Color,
{
    /// Data points to render.
    data: &'a VecDeque<u64>,
    /// Maximum value for scaling.
    max: u64,
    /// Height in rows.
    height: u16,
    /// Function to compute color for a value given (value, max).
    color_fn: F,
}

impl<'a, F> AsciiDotGraph<'a, F>
where
    F: Fn(u64, u64) -> Color,
{
    /// Create a new AsciiDotGraph widget.
    pub fn new(data: &'a VecDeque<u64>, max: u64, color_fn: F) -> Self {
        Self {
            data,
            max: max.max(1),
            height: DEFAULT_HEIGHT,
            color_fn,
        }
    }

    /// Set the height of the graph.
    pub fn height(mut self, height: u16) -> Self {
        self.height = height.max(1);
        self
    }

    /// Build the grid of lines for rendering.
    fn build_grid(&self, width: usize) -> Vec<Line<'static>> {
        let height = self.height as usize;
        if height == 0 {
            return vec![];
        }

        let data_len = self.data.len();
        let start_idx = data_len.saturating_sub(width);

        // Initialize grid with spaces
        let mut grid: Vec<Vec<Span<'static>>> = (0..height)
            .map(|_| vec![Span::raw(" ".to_string()); width])
            .collect();

        // Fill in bars for each data point
        for col in 0..width {
            let data_idx = start_idx + col;
            let value = if data_idx < data_len {
                self.data.get(data_idx).copied().unwrap_or(0)
            } else {
                0
            };

            if value == 0 {
                // For zero values, show a dim dot at the bottom only
                let row = height - 1;
                grid[row][col] = Span::styled(DOT_SINGLE.to_string(), Style::default().fg(ZERO_COLOR));
            } else {
                // Calculate top_row: higher values = smaller row number (closer to top)
                // top_row = H - 1 - (v * (H - 1) / max)
                let h_minus_1 = (height - 1) as f64;
                let normalized = (value as f64 / self.max as f64).min(1.0);
                let top_row = (h_minus_1 - (normalized * h_minus_1)).round() as usize;
                let top_row = top_row.min(height - 1);

                // Get color for this value
                let color = (self.color_fn)(value, self.max);

                // Use '.' for peak (top), ':' for fill below
                let peak_span = Span::styled(DOT_SINGLE.to_string(), Style::default().fg(color));
                let fill_span = Span::styled(DOT_DOUBLE.to_string(), Style::default().fg(color));

                // Draw peak at top_row
                grid[top_row][col] = peak_span;

                // Fill from top_row+1 down to baseline with ':'
                for row in grid.iter_mut().take(height).skip(top_row + 1) {
                    row[col] = fill_span.clone();
                }
            }
        }

        // Convert grid to lines
        grid.into_iter().map(Line::from).collect()
    }
}

impl<F> Widget for AsciiDotGraph<'_, F>
where
    F: Fn(u64, u64) -> Color,
{
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.width == 0 || area.height == 0 {
            return;
        }

        // Adjust height to fit available space
        let actual_height = (self.height as usize).min(area.height as usize);
        let graph = Self {
            data: self.data,
            max: self.max,
            height: actual_height as u16,
            color_fn: self.color_fn,
        };
        let lines = graph.build_grid(area.width as usize);

        // Only render the lines that fit
        let lines_to_render: Vec<Line> = lines.into_iter().take(actual_height).collect();

        Paragraph::new(lines_to_render).render(area, buf);
    }
}

/// Standard threshold-based color function.
pub fn make_color_fn(red_cap: u64, dynamic: bool) -> impl Fn(u64, u64) -> Color {
    move |value: u64, observed_max: u64| {
        if value == 0 {
            return Color::Rgb(40, 60, 40); // Dim green for zeros
        }

        let effective_max = if dynamic {
            observed_max.max(1)
        } else {
            red_cap.max(1)
        };

        let pct = (value as f64 / effective_max as f64 * 100.0).min(100.0);
        let above_cap = value >= red_cap;

        if above_cap || pct > 90.0 {
            Color::Rgb(255, 80, 80) // Red
        } else if pct > 66.0 {
            Color::Rgb(255, 165, 0) // Orange
        } else if pct > 33.0 {
            Color::Rgb(255, 255, 0) // Yellow
        } else {
            Color::Rgb(80, 255, 80) // Green
        }
    }
}

// Keep Dotline for backwards compatibility but mark as deprecated
#[allow(dead_code)]
pub struct Dotline<'a, F>
where
    F: Fn(u64, u64) -> Color,
{
    data: &'a VecDeque<u64>,
    max: u64,
    color_fn: F,
}

#[allow(dead_code)]
impl<'a, F> Dotline<'a, F>
where
    F: Fn(u64, u64) -> Color,
{
    pub fn new(data: &'a VecDeque<u64>, max: u64, color_fn: F) -> Self {
        Self { data, max, color_fn }
    }
}
