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
        self.color_schemes = {
            'default': {
                'before': 'green',
                'during': 'red', 
                'after': 'blue',
                'background': 'white',
                'grid': 'lightgray',
                'text': 'black'
            },
            'publication': {
                'before': '#2E8B57',  # Sea green
                'during': '#DC143C',  # Crimson
                'after': '#4682B4',   # Steel blue
                'background': 'white',
                'grid': '#F0F0F0',
                'text': '#333333'
            },
            'presentation': {
                'before': '#228B22',  # Forest green
                'during': '#FF4500',  # Orange red
                'after': '#1E90FF',   # Dodger blue
                'background': '#FAFAFA',
                'grid': '#E0E0E0',
                'text': '#2F2F2F'
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
        """Apply style to matplotlib"""
        if style_name not in self.plot_styles:
            style_name = 'default'
            
        style = self.plot_styles[style_name]
        
        plt.rcParams.update({
            'figure.figsize': style['figure_size'],
            'figure.dpi': style['dpi'],
            'lines.linewidth': style['line_width'],
            'font.size': style['font_size'],
            'axes.titlesize': style['title_size'],
            'axes.labelsize': style['font_size'],
            'xtick.labelsize': style['font_size'] - 1,
            'ytick.labelsize': style['font_size'] - 1,
            'legend.fontsize': style['font_size'] - 1,
            'figure.titlesize': style['title_size'] + 2
        })
    
    def get_colors(self, style_name='default'):
        """Get color scheme for a style"""
        return self.color_schemes.get(style_name, self.color_schemes['default'])
    
    def get_style_config(self, style_name='default'):
        """Get complete style configuration"""
        return self.plot_styles.get(style_name, self.plot_styles['default'])

# Global instance
styles_gallery = StylesGallery()