#!/usr/bin/env python3
"""
Styles Gallery Integration for Transient Service
Provides consistent styling across all analysis services
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np
from pathlib import Path
import os

class StylesGallery:
    """Centralized styling system for consistent visualization across services"""
    
    def __init__(self):
        # UNIVERSAL COLOR PALETTE - Demo Standards
        self.color_schemes = {
            'default': {
                'primary': '#1f77b4',      # Universal primary blue
                'before': '#2ca02c',       # Universal green
                'during': '#d62728',       # Universal red
                'after': '#1f77b4',        # Universal blue
                'background': '#ffffff',    # Universal white background
                'grid': '#f0f0f0',         # Universal light gray
                'text': '#333333',         # Universal dark gray text
                'secondary': '#ff7f0e',    # Universal orange
                'accent': '#9467bd'        # Universal purple
            },
            'publication': {
                'primary': '#1f77b4',
                'before': '#2ca02c',
                'during': '#d62728', 
                'after': '#1f77b4',
                'background': '#ffffff',
                'grid': '#f8f9fa',
                'text': '#212529',
                'secondary': '#ff7f0e',
                'accent': '#9467bd'
            },
            'presentation': {
                'primary': '#1f77b4',
                'before': '#2ca02c',
                'during': '#d62728',
                'after': '#1f77b4',
                'background': '#ffffff',
                'grid': '#e9ecef',
                'text': '#495057',
                'secondary': '#ff7f0e',
                'accent': '#9467bd'
            }
        }
        
        self.plot_styles = {
            'default': {
                'figure_size': (20, 32),
                'dpi': 150,
                'line_width': 1.5,
                'grid_alpha': 0.3,
                'font_size': 10,
                'title_size': 12
            },
            'publication': {
                'figure_size': (12, 16),
                'dpi': 300,
                'line_width': 2.0,
                'grid_alpha': 0.2,
                'font_size': 12,
                'title_size': 14
            },
            'presentation': {
                'figure_size': (16, 20),
                'dpi': 200,
                'line_width': 2.5,
                'grid_alpha': 0.25,
                'font_size': 14,
                'title_size': 16
            }
        }
    
    def apply_style(self, style_name='default'):
        """Apply universal demo-ready style to matplotlib"""
        if style_name not in self.plot_styles:
            style_name = 'default'
            
        style = self.plot_styles[style_name]
        colors = self.color_schemes[style_name]
        
        # UNIVERSAL DEMO STYLING
        plt.rcParams.update({
            'figure.figsize': style['figure_size'],
            'figure.dpi': style['dpi'],
            'figure.facecolor': colors['background'],
            'axes.facecolor': colors['background'],
            'axes.edgecolor': colors['text'],
            'axes.labelcolor': colors['text'],
            'axes.grid': True,
            'grid.color': colors['grid'],
            'grid.alpha': style['grid_alpha'],
            'lines.linewidth': style['line_width'],
            'font.size': style['font_size'],
            'font.family': 'sans-serif',
            'font.sans-serif': ['Arial', 'DejaVu Sans', 'Liberation Sans', 'sans-serif'],
            'axes.titlesize': style['title_size'],
            'axes.labelsize': style['font_size'],
            'xtick.labelsize': style['font_size'] - 1,
            'ytick.labelsize': style['font_size'] - 1,
            'xtick.color': colors['text'],
            'ytick.color': colors['text'],
            'legend.fontsize': style['font_size'] - 1,
            'legend.frameon': True,
            'legend.fancybox': True,
            'legend.shadow': True,
            'figure.titlesize': style['title_size'] + 2,
            'axes.prop_cycle': plt.cycler('color', [
                colors['primary'], colors['secondary'], colors['accent'],
                colors['before'], colors['during'], colors['after']
            ])
        })
    
    def get_colors(self, style_name='default'):
        """Get color scheme for a style"""
        return self.color_schemes.get(style_name, self.color_schemes['default'])
    
    def get_style_config(self, style_name='default'):
        """Get complete style configuration"""
        return self.plot_styles.get(style_name, self.plot_styles['default'])

# Global instance
styles_gallery = StylesGallery()