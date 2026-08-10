//! Color palette definitions for Catppuccin Mocha theme used across the TUI.

use ratatui::style::Color;

/// Base background color.
pub const BASE: Color = Color::Reset;
/// Crust background color.
pub const CRUST: Color = Color::Reset;

/// Surface 0 element background color.
pub const SURFACE0: Color = Color::Rgb(49, 50, 68);
/// Surface 2 element background color.
pub const SURFACE2: Color = Color::Rgb(88, 91, 112);

/// Subtext 0 secondary text color.
pub const SUBTEXT0: Color = Color::Reset;
/// Primary text color.
pub const TEXT: Color = Color::Reset;

/// Accent color: Green.
pub const GREEN: Color = Color::Rgb(166, 227, 161);
/// Accent color: Yellow.
pub const YELLOW: Color = Color::Rgb(249, 226, 175);
/// Accent color: Red.
pub const RED: Color = Color::Rgb(243, 139, 168);
