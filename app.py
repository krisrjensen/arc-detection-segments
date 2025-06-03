#!/usr/bin/env python3
"""
Data Segment Visualizer with Caching - Version 20250531_230200_0_0_1_1
Enhanced data segment visualization system with intelligent caching support

Integrates with the cache manager to provide:
- Instant loading of cached segments and plots
- Background pre-generation for Nr/Nf window
- Cache-aware navigation and performance optimization
- Seamless fallback to real-time generation when needed
"""

import os
import sys
import time
import json
import numpy as np
import sqlite3
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from flask import Flask, render_template, request, jsonify, send_from_directory
from pathlib import Path
import base64
from io import BytesIO
import yaml
from datetime import datetime

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from v3_database import V3Database
from cache_manager_20250531_230000_0_0_1_1 import get_cache_manager

# STYLES GALLERY INTEGRATION
from styles import styles_gallery
from image_utils import universal_saver

app = Flask(__name__)

# Configuration
V3_DATABASE_PATH = "/Volumes/ArcData/V3_database/arc_detection.db"
SYNC_FILE_PATH = "/Volumes/ArcData/V3_database/current_experiment.sync"
RAW_DATA_DIR = "/Volumes/ArcData/V3_raw_data"
PLOTS_DIR = "/Users/kjensen/Documents/GitHub/data_processor_project/arc_detection_project/temp_segment_plots"

# Ensure directories exist
os.makedirs(PLOTS_DIR, exist_ok=True)

# Default configuration
DEFAULT_CONFIG = {
    "segment_lengths": [524288, 65536, 8192],
    "default_overlap": 0.0,
    "max_overlap": 50.0,
    "max_segments": 50,
    "center_strategy": "transient1",
    "id_format": {
        "left": "L{number:03d}",
        "right": "R{number:03d}",
        "center": "C001"
    },
    "special_cases": {
        "restriking": "multiple_transients"
    }
}

class CachedSegmentGenerator:
    """Enhanced segment generator with caching support"""
    
    def __init__(self, config=None):
        self.config = config or DEFAULT_CONFIG
        self.db = V3Database()
        self.cache_manager = get_cache_manager()
    
    def generate_default_segments(self, file_id, transient1_index, arc_type='normal'):
        """Generate segments with cache integration"""
        # Try to get from cache first
        cached_segments = self.cache_manager.get_cached_segments(file_id)
        if cached_segments:
            print(f"Using cached segments for file {file_id}")
            return cached_segments
        
        print(f"Generating segments for file {file_id} (cache miss)")
        
        # Generate segments using original logic
        segments = self._generate_segments_original(file_id, transient1_index, arc_type)
        
        # Cache the generated segments
        self._cache_segments(file_id, segments)
        
        return segments
    
    def _generate_segments_original(self, file_id, transient1_index, arc_type='normal'):
        """Original segment generation logic"""
        segments = []
        
        # Get file length from binary data
        file_length = self._get_file_length(file_id)
        if not file_length:
            return segments
        
        # Special handling for steady_state files - start from beginning
        if arc_type in ['steady_state', 'steady_state_motor_parallel'] or not transient1_index:
            print(f"Generating steady_state segments from beginning for file {file_id} (type: {arc_type})")
            segments = self._generate_steady_state_segments(file_id, file_length, arc_type)
        else:
            # Normal case - center on transient
            for segment_length in self.config["segment_lengths"]:
                # Generate segments for this length
                length_segments = self._generate_segments_for_length(
                    file_id, transient1_index, segment_length, file_length, arc_type
                )
                segments.extend(length_segments)
        
        return segments
    
    def _cache_segments(self, file_id, segments):
        """Cache segments for future use"""
        try:
            cache_file = os.path.join(self.cache_manager.config.get('cache_dir', '/tmp'), 
                                    'segments', f"segments_{file_id:08d}.json")
            os.makedirs(os.path.dirname(cache_file), exist_ok=True)
            
            with open(cache_file, 'w') as f:
                json.dump(segments, f, indent=2)
            
            print(f"Cached segments for file {file_id}")
        except Exception as e:
            print(f"Error caching segments for file {file_id}: {e}")
    
    def _generate_steady_state_segments(self, file_id, file_length, arc_type):
        """Generate segments for steady_state files starting from beginning"""
        segments = []
        
        # Determine appropriate data label based on arc type
        if arc_type == 'steady_state_motor_parallel':
            data_label = 'steady_state_motor_parallel'
        else:
            data_label = 'steady_state'
        
        for segment_length in self.config["segment_lengths"]:
            # Calculate how many non-overlapping segments we can fit
            max_segments = min(self.config["max_segments"], file_length // segment_length)
            
            for i in range(max_segments):
                start_index = i * segment_length
                end_index = min(start_index + segment_length, file_length)
                
                # Stop if we don't have a full segment
                if end_index - start_index < segment_length:
                    break
                
                # Generate sequential ID codes: A001, A002, A003, etc.
                segment_id_code = f"A{i+1:03d}"
                
                segment = {
                    'file_id': file_id,
                    'segment_type': 'augmented',  # Use 'augmented' type for steady_state segments
                    'segment_id_code': segment_id_code,
                    'start_index': start_index,
                    'end_index': end_index,
                    'segment_length': segment_length,
                    'data_label': data_label,
                    'overlap_percent': 0.0,
                    'transient_position': None
                }
                segments.append(segment)
        
        return segments
    
    def _generate_segments_for_length(self, file_id, center_index, segment_length, file_length, arc_type):
        """Generate segments for specific length"""
        segments = []
        half_length = segment_length // 2
        
        # Calculate center segment position
        center_start = max(0, center_index - half_length)
        center_end = min(file_length, center_start + segment_length)
        
        # Create center segment
        center_segment = {
            'file_id': file_id,
            'segment_type': 'center',
            'segment_id_code': self.config["id_format"]["center"],
            'start_index': center_start,
            'end_index': center_end,
            'segment_length': segment_length,
            'data_label': self._get_default_label(arc_type, 'center'),
            'overlap_percent': 0.0,
            'transient_position': center_index - center_start
        }
        segments.append(center_segment)
        
        # Generate left segments
        left_segments = self._generate_directional_segments(
            file_id, center_start, segment_length, file_length, 'left', arc_type
        )
        segments.extend(left_segments)
        
        # Generate right segments
        right_segments = self._generate_directional_segments(
            file_id, center_end, segment_length, file_length, 'right', arc_type
        )
        segments.extend(right_segments)
        
        return segments
    
    def _generate_directional_segments(self, file_id, start_pos, segment_length, file_length, direction, arc_type):
        """Generate segments in specified direction (left/right)"""
        segments = []
        max_segments = self.config["max_segments"] // 2  # Half for each direction
        
        for i in range(1, max_segments + 1):
            if direction == 'left':
                seg_end = start_pos
                seg_start = max(0, seg_end - segment_length)
                if seg_start == 0 and seg_end - seg_start < segment_length // 2:
                    break  # Stop if segment would be too small
                id_code = self.config["id_format"]["left"].format(number=i)
                start_pos = seg_start  # Update for next iteration
            else:  # right
                seg_start = start_pos
                seg_end = min(file_length, seg_start + segment_length)
                if seg_end == file_length and seg_end - seg_start < segment_length // 2:
                    break  # Stop if segment would be too small
                id_code = self.config["id_format"]["right"].format(number=i)
                start_pos = seg_end  # Update for next iteration
            
            segment = {
                'file_id': file_id,
                'segment_type': direction,
                'segment_id_code': id_code,
                'start_index': seg_start,
                'end_index': seg_end,
                'segment_length': segment_length,
                'data_label': self._get_default_label(arc_type, direction),
                'overlap_percent': 0.0,
                'transient_position': None
            }
            segments.append(segment)
            
            # Stop if we've reached file boundaries
            if (direction == 'left' and seg_start == 0) or (direction == 'right' and seg_end == file_length):
                break
        
        return segments
    
    def _get_file_length(self, file_id):
        """Get file length from database or binary data"""
        try:
            # Try to get from database first
            conn = sqlite3.connect(V3_DATABASE_PATH)
            cursor = conn.cursor()
            cursor.execute('SELECT binary_data_path FROM files WHERE file_id = ?', (file_id,))
            result = cursor.fetchone()
            conn.close()
            
            if result and result[0]:
                binary_path = Path(f"/Volumes/ArcData/V3_database/fileset/{file_id:08d}.npy")
                if binary_path.exists():
                    data = np.load(binary_path)
                    return len(data) if len(data.shape) == 1 else data.shape[0]
            
            # Default to standard length from CLAUDE.md
            return 2500000  # 2.5M points for 0.5 seconds at 5MSPS
            
        except Exception as e:
            print(f"Error getting file length for {file_id}: {e}")
            return 2500000
    
    def _get_default_label(self, arc_type, segment_type):
        """Get default data label based on arc type and segment position"""
        label_map = {
            'normal': {
                'left': 'steady_state',
                'center': 'arc_transient',
                'right': 'steady_state'
            },
            'restriking': {
                'left': 'steady_state',
                'center': 'restriking_arc',
                'right': 'steady_state'
            },
            'negative_transient': {
                'left': 'steady_state',
                'center': 'negative_transient',
                'right': 'steady_state'
            },
            'steady_state': {
                'left': 'steady_state',
                'center': 'steady_state',
                'right': 'steady_state'
            }
        }
        
        return label_map.get(arc_type, label_map['normal']).get(segment_type, 'unknown')

class CachedSegmentPlotter:
    """Enhanced segment plotter with caching support"""
    
    def __init__(self, plots_dir):
        self.plots_dir = plots_dir
        self.cache_manager = get_cache_manager()
    
    def create_time_series_rectangle_plots(self, file_id, segments_by_length):
        """Create time-series plots with cache integration"""
        # Try to get cached plots first
        cached_plots = self.cache_manager.get_cached_plots(file_id)
        if cached_plots:
            # Verify cached plot files still exist
            all_exist = True
            for plot_file in cached_plots.values():
                if not os.path.exists(os.path.join(self.plots_dir, plot_file)):
                    all_exist = False
                    break
            
            if all_exist:
                print(f"Using cached plots for file {file_id}")
                return cached_plots
        
        print(f"Generating plots for file {file_id} (cache miss)")
        
        # Generate plots using original logic
        plot_files = self._create_plots_original(file_id, segments_by_length)
        
        return plot_files
    
    def _create_plots_original(self, file_id, segments_by_length):
        """Original plot creation logic"""
        plot_files = {}
        
        # Calculate the maximum time range across all segment lengths for alignment
        max_time_end = 0
        sampling_rate = 5000000  # 5 MSPS
        
        for segments in segments_by_length.values():
            if segments:
                time_end = max(seg['end_index'] for seg in segments) / sampling_rate
                max_time_end = max(max_time_end, time_end)
        
        for segment_length, segments in segments_by_length.items():
            plot_file = self._plot_segment_rectangles(file_id, segments, segment_length, max_time_end)
            plot_files[segment_length] = plot_file
        
        return plot_files
    
    def _plot_segment_rectangles(self, file_id, segments, segment_length, max_time_end=None):
        """Plot rectangles for specific segment length on time axis"""
        # Convert sample indices to time (assuming 5MSPS from CLAUDE.md)
        sampling_rate = 5000000  # 5 MSPS
        
        # Create figure (normal aspect ratio)
        fig, ax = plt.subplots(1, 1, figsize=(16, 4))
        
        # Use provided max_time_end for alignment, or calculate if not provided
        if max_time_end is not None:
            time_end = max_time_end
        else:
            max_end = max(seg['end_index'] for seg in segments) if segments else segment_length
            time_end = max_end / sampling_rate
        
        # Plot each segment as a rectangle
        colors = {'left': '#FF6B6B', 'center': '#4ECDC4', 'right': '#45B7D1', 'augmented': '#9B59B6'}
        y_pos = 0.5
        rect_height = 0.4
        
        # Collect unique data labels for legend
        unique_labels = {}
        for segment in segments:
            label = segment['data_label'] or 'unknown'  # Handle None values
            if label not in unique_labels:
                unique_labels[label] = colors.get(segment['segment_type'], '#95A5A6')
        
        # Determine if we need to rotate segment IDs (more than 16 segments)
        rotate_ids = len(segments) > 16
        
        for segment in segments:
            start_time = segment['start_index'] / sampling_rate
            end_time = segment['end_index'] / sampling_rate
            width = end_time - start_time
            
            # Get color based on segment type
            color = colors.get(segment['segment_type'], '#95A5A6')
            
            # Create rectangle
            rect = patches.Rectangle(
                (start_time, y_pos), width, rect_height,
                linewidth=1, edgecolor='black', facecolor=color, alpha=0.7
            )
            ax.add_patch(rect)
            
            # Add segment ID label (rotate if too many segments)
            label_x = start_time + width / 2
            label_y = y_pos + rect_height / 2
            
            # Handle None segment_id_code
            segment_id = segment['segment_id_code'] or f"S{segment.get('segment_id', '?')}"
            
            if rotate_ids:
                ax.text(label_x, label_y, segment_id, 
                       ha='center', va='center', fontsize=12, fontweight='bold', 
                       color='white', rotation=90)
            else:
                ax.text(label_x, label_y, segment_id, 
                       ha='center', va='center', fontsize=16, fontweight='bold', color='white')
        
        # Set axis properties (cleaner layout without space for labels below)
        ax.set_xlim(0, time_end)
        ax.set_ylim(0.3, 1.0)
        ax.set_xlabel('Time (seconds)')
        ax.set_ylabel('')
        ax.set_title(f'Data Segments - Length: {segment_length} samples')
        ax.set_yticks([])
        ax.grid(True, alpha=0.3)
        
        # Create legend with data labels instead of segment types
        legend_elements = []
        for label, color in unique_labels.items():
            # Handle None or empty labels safely
            display_label = str(label) if label else 'Unknown'
            formatted_label = display_label.replace('_', ' ').title()
            legend_elements.append(patches.Patch(color=color, label=formatted_label))
        
        ax.legend(handles=legend_elements, loc='upper right', fontsize=10)
        
        plt.tight_layout()
        
        # UNIVERSAL IMAGE SAVE: Use universal saver with metadata
        segment_metadata = {
            'plot_type': 'data_segments',
            'file_id': file_id,
            'segment_length': segment_length,
            'segment_count': len(segments),
            'sampling_rate': sampling_rate,
            'max_time_end': max_time_end,
            'service': 'data_segment_visualizer'
        }
        
        save_result = universal_saver.save_plot(
            filename=f"segments_file_{file_id:08d}_length_{segment_length}",
            experiment_name=f"file_{file_id:08d}",
            style='default',
            formats=['png'],
            dpi=150,
            bbox_inches='tight',
            metadata=segment_metadata
        )
        
        # For backward compatibility, also save to temp plots directory
        plot_filename = f"segments_file_{file_id:08d}_length_{segment_length}.png"
        plot_path = os.path.join(self.plots_dir, plot_filename)
        plt.savefig(plot_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        print(f"UNIVERSAL SAVE: Segment plot saved: {save_result.get('primary_file')}")
        
        return plot_filename

class CachedSyncManager:
    """Enhanced sync manager with cache integration"""
    
    def __init__(self):
        self.current_experiment = None
        self.current_file_id = None
        self.cache_manager = get_cache_manager()
    
    def get_current_experiment(self):
        """Read current experiment from sync file and update cache"""
        try:
            if os.path.exists(SYNC_FILE_PATH):
                with open(SYNC_FILE_PATH, 'r') as f:
                    experiment_path = f.read().strip()
                    if experiment_path and experiment_path != self.current_experiment:
                        self.current_experiment = experiment_path
                        old_file_id = self.current_file_id
                        self.current_file_id = self._extract_file_id(experiment_path)
                        
                        # Update cache manager with new current file
                        if self.current_file_id and self.current_file_id != old_file_id:
                            self.cache_manager.update_current_file(self.current_file_id)
                        
                        return True  # Experiment changed
            return False  # No change
        except Exception as e:
            print(f"Error reading sync file: {e}")
            return False
    
    def _extract_file_id(self, experiment_path):
        """Extract file_id from experiment path"""
        try:
            # Get filename from path
            filename = experiment_path.split('/')[-1] if '/' in experiment_path else experiment_path
            
            # Query database for file_id
            conn = sqlite3.connect(V3_DATABASE_PATH)
            cursor = conn.cursor()
            cursor.execute('SELECT file_id FROM files WHERE original_filename LIKE ?', (f'%{filename}%',))
            result = cursor.fetchone()
            conn.close()
            
            return result[0] if result else None
        except Exception as e:
            print(f"Error extracting file_id: {e}")
            return None

# Global instances with caching
sync_manager = CachedSyncManager()
segment_generator = CachedSegmentGenerator()
segment_plotter = CachedSegmentPlotter(PLOTS_DIR)

# Import all original routes and modify key functions
@app.route('/')
def index():
    """Main segment visualization interface with cache status"""
    # Check for current experiment
    sync_manager.get_current_experiment()
    
    # Get cache status
    cache_mgr = get_cache_manager()
    cache_window = cache_mgr.config.get_cache_window()
    
    return render_template('data_segment_viewer_cached_v1.html',
                         current_experiment=sync_manager.current_experiment,
                         current_file_id=sync_manager.current_file_id,
                         config=DEFAULT_CONFIG,
                         cache_window=cache_window)

@app.route('/api/cache/status')
def get_cache_status():
    """Get cache status for current file"""
    cache_mgr = get_cache_manager()
    return jsonify({
        'success': True,
        'cache_status': cache_mgr.get_status_summary(),
        'cache_window': cache_mgr.config.get_cache_window()
    })

@app.route('/segments/<int:file_id>')
def get_segments(file_id):
    """Get segments for specific file with caching"""
    try:
        # Try cache first
        cache_mgr = get_cache_manager()
        cached_segments = cache_mgr.get_cached_segments(file_id)
        
        if cached_segments:
            return jsonify({
                'success': True,
                'segments': cached_segments,
                'cached': True
            })
        
        # Check if segments exist in database
        conn = sqlite3.connect(V3_DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT segment_id, segment_type, segment_id_code, beginning_index, 
                   (beginning_index + segment_length) as end_index,
                   segment_length, data_label, overlap_percentage
            FROM data_segments WHERE experiment_file_id = ?
        ''', (file_id,))
        existing_segments = cursor.fetchall()
        conn.close()
        
        if not existing_segments:
            # Auto-generate segments
            segments = generate_segments_for_file(file_id)
            return jsonify({
                'success': True,
                'segments': segments,
                'auto_generated': True,
                'cached': False
            })
        else:
            # Return existing segments, handling None values
            segments = []
            for row in existing_segments:
                segment = {
                    'segment_id': row[0],
                    'segment_type': row[1] or 'unknown',
                    'segment_id_code': row[2] or f"S{row[0]}",
                    'start_index': row[3],
                    'end_index': row[4],
                    'segment_length': row[5],
                    'data_label': row[6] or 'unknown',
                    'overlap_percent': row[7] or 0.0
                }
                segments.append(segment)
            
            return jsonify({
                'success': True,
                'segments': segments,
                'auto_generated': False,
                'cached': False
            })
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/plots/rectangles/<int:file_id>')
def get_rectangle_plots(file_id):
    """Get rectangle plots for file with caching"""
    try:
        # Get segments grouped by length
        segments_response = get_segments(file_id)
        if not segments_response.json['success']:
            return segments_response
        
        segments = segments_response.json['segments']
        was_cached = segments_response.json.get('cached', False)
        
        # Group by segment length
        segments_by_length = {}
        for segment in segments:
            length = segment['segment_length']
            if length not in segments_by_length:
                segments_by_length[length] = []
            segments_by_length[length].append(segment)
        
        # Generate plots (with caching)
        plot_files = segment_plotter.create_time_series_rectangle_plots(file_id, segments_by_length)
        
        return jsonify({
            'success': True,
            'plot_files': plot_files,
            'segment_lengths': list(segments_by_length.keys()),
            'cached': was_cached
        })
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# Import remaining routes from original file
from data_segment_visualizer_20250531_140000_0_0_1_1 import (
    sync_experiment, save_segments_to_database, generate_segments_for_file,
    reset_segments, generate_segments_endpoint, save_segments, status, serve_plot
)

# Add these routes
app.add_url_rule('/sync', 'sync_experiment', sync_experiment, methods=['POST'])
app.add_url_rule('/segments/reset', 'reset_segments', reset_segments, methods=['POST'])
app.add_url_rule('/segments/generate', 'generate_segments_endpoint', generate_segments_endpoint, methods=['POST'])
app.add_url_rule('/segments/save', 'save_segments', save_segments, methods=['POST'])
app.add_url_rule('/status', 'status', status)
app.add_url_rule('/plot/<filename>', 'serve_plot', serve_plot)

if __name__ == '__main__':
    print("=== CACHED DATA SEGMENT VISUALIZER ===")
    print(f"Database: {V3_DATABASE_PATH}")
    print(f"Sync file: {SYNC_FILE_PATH}")
    print(f"Plots directory: {PLOTS_DIR}")
    print(f"Server: http://localhost:5032")
    print("Enhanced with intelligent caching system...")
    
    # Initialize cache manager
    cache_mgr = get_cache_manager()
    Nr, Nf = cache_mgr.config.get_cache_window()
    print(f"Cache window: Nr={Nr} (behind), Nf={Nf} (forward)")
    
    app.run(debug=True, host='0.0.0.0', port=5032)