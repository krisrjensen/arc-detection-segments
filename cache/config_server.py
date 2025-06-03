#!/usr/bin/env python3
"""
Cache Configuration Server - Version 20250531_230100_0_0_1_1
Web interface for managing cache configuration and monitoring

Provides a web UI for:
- Adjusting Nr (rear) and Nf (forward) cache window parameters
- Monitoring cache generation status and performance
- Managing cache cleanup and settings
- Real-time cache statistics and queue status
"""

import os
import sys
import json
import time
from flask import Flask, render_template, request, jsonify, send_from_directory
from pathlib import Path

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from cache_manager_20250531_230000_0_0_1_1 import get_cache_manager, CacheConfiguration

app = Flask(__name__)

# Configuration
CACHE_CONFIG_PORT = 5025

@app.route('/')
def index():
    """Main cache configuration interface"""
    cache_mgr = get_cache_manager()
    config = cache_mgr.config.config
    status_summary = cache_mgr.get_status_summary()
    
    return render_template('cache_config_v1.html',
                         config=config,
                         status=status_summary)

@app.route('/api/config')
def get_config():
    """Get current cache configuration"""
    cache_mgr = get_cache_manager()
    return jsonify({
        'success': True,
        'config': cache_mgr.config.config
    })

@app.route('/api/config/update', methods=['POST'])
def update_config():
    """Update cache configuration"""
    try:
        data = request.json
        cache_mgr = get_cache_manager()
        
        # Update specific configuration values
        for key_path, value in data.items():
            cache_mgr.config.set(key_path, value)
        
        return jsonify({
            'success': True,
            'message': 'Configuration updated successfully',
            'config': cache_mgr.config.config
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/cache/window/update', methods=['POST'])
def update_cache_window():
    """Update cache window (Nr, Nf) parameters"""
    try:
        data = request.json
        Nr = int(data.get('Nr', 3))
        Nf = int(data.get('Nf', 10))
        
        # Validate values
        if Nr < 0 or Nr > 50:
            return jsonify({'success': False, 'error': 'Nr must be between 0 and 50'})
        if Nf < 0 or Nf > 100:
            return jsonify({'success': False, 'error': 'Nf must be between 0 and 100'})
        
        cache_mgr = get_cache_manager()
        cache_mgr.config.set('cache_window.Nr', Nr)
        cache_mgr.config.set('cache_window.Nf', Nf)
        
        return jsonify({
            'success': True,
            'message': f'Cache window updated: Nr={Nr}, Nf={Nf}',
            'cache_window': {'Nr': Nr, 'Nf': Nf}
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/status')
def get_status():
    """Get current cache status"""
    cache_mgr = get_cache_manager()
    return jsonify({
        'success': True,
        'status': cache_mgr.get_status_summary()
    })

@app.route('/api/cache/current-file/update', methods=['POST'])
def update_current_file():
    """Update current file and trigger cache generation"""
    try:
        data = request.json
        file_id = int(data.get('file_id'))
        
        cache_mgr = get_cache_manager()
        cache_mgr.update_current_file(file_id)
        
        return jsonify({
            'success': True,
            'message': f'Current file updated to {file_id}',
            'file_id': file_id
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/cache/cleanup', methods=['POST'])
def cleanup_cache():
    """Trigger cache cleanup"""
    try:
        cache_mgr = get_cache_manager()
        cache_mgr.cleanup_cache()
        
        return jsonify({
            'success': True,
            'message': 'Cache cleanup completed'
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/cache/stats')
def get_cache_stats():
    """Get detailed cache statistics"""
    cache_mgr = get_cache_manager()
    status = cache_mgr.status.status
    
    # Calculate additional statistics
    total_files = len(cache_mgr.file_manager.get_file_sequence())
    cached_files = len(status.get('completed', {}))
    cache_coverage = (cached_files / total_files * 100) if total_files > 0 else 0
    
    return jsonify({
        'success': True,
        'stats': {
            'total_files': total_files,
            'cached_files': cached_files,
            'cache_coverage_percent': round(cache_coverage, 1),
            'queue_length': len(status.get('generation_queue', [])),
            'in_progress': len(status.get('in_progress', {})),
            'failed_files': len(status.get('failed', {})),
            'cache_hits': status.get('stats', {}).get('cache_hits', 0),
            'cache_misses': status.get('stats', {}).get('cache_misses', 0),
            'avg_generation_time': round(status.get('stats', {}).get('generation_time_avg', 0), 2)
        }
    })

@app.route('/api/files/sequence')
def get_file_sequence():
    """Get file sequence for navigation"""
    cache_mgr = get_cache_manager()
    file_sequence = cache_mgr.file_manager.get_file_sequence()
    
    return jsonify({
        'success': True,
        'file_sequence': file_sequence[:100],  # Limit to first 100 for UI
        'total_files': len(file_sequence)
    })

if __name__ == '__main__':
    print("=== CACHE CONFIGURATION SERVER ===")
    print(f"Server: http://localhost:{CACHE_CONFIG_PORT}")
    print("Configure cache settings and monitor performance...")
    
    app.run(debug=True, host='0.0.0.0', port=CACHE_CONFIG_PORT)