#!/usr/bin/env python3
"""
Cache Manager - Version 20250531_230000_0_0_1_1
Intelligent caching system for arc detection data processing

Provides configurable pre-generation of segments and plots for Nr records behind 
and Nf records in front of current file for rapid data approval workflows.

Key Features:
- Configurable cache window (Nr=3 rear, Nf=10 forward by default)
- Background pre-generation of segments and plots
- Cache invalidation and cleanup mechanisms
- Thread-safe operations with status monitoring
- Integration with existing visualization tools
"""

import os
import sys
import time
import json
import threading
import sqlite3
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Set
import logging

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Configuration
V3_DATABASE_PATH = "/Volumes/ArcData/V3_database/arc_detection.db"
CACHE_DIR = "/Users/kjensen/Documents/GitHub/data_processor_project/arc_detection_project/cache"
PLOTS_CACHE_DIR = os.path.join(CACHE_DIR, "plots")
SEGMENTS_CACHE_DIR = os.path.join(CACHE_DIR, "segments")
VERIFICATION_CACHE_DIR = os.path.join(CACHE_DIR, "verification")

# Ensure cache directories exist
for cache_dir in [CACHE_DIR, PLOTS_CACHE_DIR, SEGMENTS_CACHE_DIR, VERIFICATION_CACHE_DIR]:
    os.makedirs(cache_dir, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(CACHE_DIR, 'cache_manager.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CacheConfiguration:
    """Manages cache configuration and settings"""
    
    def __init__(self, config_file=None):
        self.config_file = config_file or os.path.join(CACHE_DIR, "cache_config.json")
        self.config = self._load_default_config()
        self._load_config()
    
    def _load_default_config(self) -> Dict:
        """Load default cache configuration"""
        return {
            "cache_window": {
                "Nr": 3,  # Records behind current file
                "Nf": 10  # Records in front of current file
            },
            "cache_types": {
                "segments": True,
                "plots": True,
                "verification": True,
                "transient_plots": False  # Disabled by default due to size
            },
            "cache_limits": {
                "max_cache_size_gb": 5.0,
                "max_cache_age_hours": 24,
                "cleanup_threshold": 0.8
            },
            "performance": {
                "max_workers": 4,
                "segment_generation_timeout": 30,
                "plot_generation_timeout": 60
            },
            "auto_cleanup": True,
            "enable_background_generation": True
        }
    
    def _load_config(self):
        """Load configuration from file if it exists"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    saved_config = json.load(f)
                    # Merge with defaults to ensure all keys exist
                    self._deep_merge(self.config, saved_config)
                logger.info(f"Loaded cache configuration from {self.config_file}")
        except Exception as e:
            logger.warning(f"Error loading cache config: {e}, using defaults")
    
    def _deep_merge(self, default: Dict, override: Dict):
        """Deep merge override config into default config"""
        for key, value in override.items():
            if key in default and isinstance(default[key], dict) and isinstance(value, dict):
                self._deep_merge(default[key], value)
            else:
                default[key] = value
    
    def save_config(self):
        """Save current configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
            logger.info(f"Saved cache configuration to {self.config_file}")
        except Exception as e:
            logger.error(f"Error saving cache config: {e}")
    
    def get(self, key_path: str, default=None):
        """Get configuration value using dot notation (e.g., 'cache_window.Nr')"""
        keys = key_path.split('.')
        value = self.config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value
    
    def set(self, key_path: str, value):
        """Set configuration value using dot notation"""
        keys = key_path.split('.')
        config = self.config
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        config[keys[-1]] = value
        self.save_config()
    
    def get_cache_window(self) -> Tuple[int, int]:
        """Get cache window (Nr, Nf)"""
        return (self.get('cache_window.Nr', 3), self.get('cache_window.Nf', 10))

class CacheStatus:
    """Tracks cache generation status and statistics"""
    
    def __init__(self):
        self.status_file = os.path.join(CACHE_DIR, "cache_status.json")
        self.lock = threading.Lock()
        self.status = {
            "current_file_id": None,
            "cache_window": {"Nr": 3, "Nf": 10},
            "generation_queue": [],
            "completed": {},
            "failed": {},
            "in_progress": {},
            "stats": {
                "total_generated": 0,
                "cache_hits": 0,
                "cache_misses": 0,
                "generation_time_avg": 0.0
            },
            "last_updated": datetime.now().isoformat()
        }
        self._load_status()
    
    def _load_status(self):
        """Load status from file"""
        try:
            if os.path.exists(self.status_file):
                with open(self.status_file, 'r') as f:
                    self.status.update(json.load(f))
        except Exception as e:
            logger.warning(f"Error loading cache status: {e}")
    
    def save_status(self):
        """Save status to file"""
        try:
            with self.lock:
                self.status["last_updated"] = datetime.now().isoformat()
                with open(self.status_file, 'w') as f:
                    json.dump(self.status, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving cache status: {e}")
    
    def update_current_file(self, file_id: int):
        """Update current file ID"""
        with self.lock:
            self.status["current_file_id"] = file_id
            self.save_status()
    
    def add_to_queue(self, file_ids: List[int]):
        """Add file IDs to generation queue"""
        with self.lock:
            current_queue = set(self.status["generation_queue"])
            new_files = [fid for fid in file_ids if fid not in current_queue]
            self.status["generation_queue"].extend(new_files)
            self.save_status()
    
    def mark_in_progress(self, file_id: int, cache_type: str):
        """Mark cache generation as in progress"""
        with self.lock:
            if file_id not in self.status["in_progress"]:
                self.status["in_progress"][file_id] = {}
            self.status["in_progress"][file_id][cache_type] = {
                "started": datetime.now().isoformat()
            }
            self.save_status()
    
    def mark_completed(self, file_id: int, cache_type: str, generation_time: float):
        """Mark cache generation as completed"""
        with self.lock:
            # Remove from in_progress
            if file_id in self.status["in_progress"]:
                self.status["in_progress"][file_id].pop(cache_type, None)
                if not self.status["in_progress"][file_id]:
                    del self.status["in_progress"][file_id]
            
            # Add to completed
            if file_id not in self.status["completed"]:
                self.status["completed"][file_id] = {}
            self.status["completed"][file_id][cache_type] = {
                "completed": datetime.now().isoformat(),
                "generation_time": generation_time
            }
            
            # Update stats
            self.status["stats"]["total_generated"] += 1
            current_avg = self.status["stats"]["generation_time_avg"]
            total = self.status["stats"]["total_generated"]
            self.status["stats"]["generation_time_avg"] = (current_avg * (total - 1) + generation_time) / total
            
            self.save_status()
    
    def mark_failed(self, file_id: int, cache_type: str, error: str):
        """Mark cache generation as failed"""
        with self.lock:
            # Remove from in_progress
            if file_id in self.status["in_progress"]:
                self.status["in_progress"][file_id].pop(cache_type, None)
                if not self.status["in_progress"][file_id]:
                    del self.status["in_progress"][file_id]
            
            # Add to failed
            if file_id not in self.status["failed"]:
                self.status["failed"][file_id] = {}
            self.status["failed"][file_id][cache_type] = {
                "failed": datetime.now().isoformat(),
                "error": error
            }
            self.save_status()
    
    def is_cached(self, file_id: int, cache_type: str) -> bool:
        """Check if file is cached for specific type"""
        return (file_id in self.status["completed"] and 
                cache_type in self.status["completed"][file_id])
    
    def get_queue_status(self) -> Dict:
        """Get current queue status"""
        with self.lock:
            return {
                "queue_length": len(self.status["generation_queue"]),
                "in_progress": len(self.status["in_progress"]),
                "completed_files": len(self.status["completed"]),
                "failed_files": len(self.status["failed"])
            }

class FileSequenceManager:
    """Manages file sequence and determines cache targets"""
    
    def __init__(self):
        self.db_path = V3_DATABASE_PATH
    
    def get_file_sequence(self) -> List[int]:
        """Get ordered list of all file IDs from database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('SELECT file_id FROM files ORDER BY file_id')
            file_ids = [row[0] for row in cursor.fetchall()]
            conn.close()
            return file_ids
        except Exception as e:
            logger.error(f"Error getting file sequence: {e}")
            return []
    
    def get_cache_targets(self, current_file_id: int, Nr: int, Nf: int) -> List[int]:
        """Get list of file IDs that should be cached for given current file"""
        file_sequence = self.get_file_sequence()
        
        if current_file_id not in file_sequence:
            logger.warning(f"Current file ID {current_file_id} not in sequence")
            return []
        
        current_index = file_sequence.index(current_file_id)
        
        # Calculate range
        start_index = max(0, current_index - Nr)
        end_index = min(len(file_sequence), current_index + Nf + 1)  # +1 to include current
        
        cache_targets = file_sequence[start_index:end_index]
        
        logger.info(f"Cache targets for file {current_file_id}: "
                   f"{len(cache_targets)} files ({start_index} to {end_index-1})")
        
        return cache_targets
    
    def get_next_file(self, current_file_id: int) -> Optional[int]:
        """Get next file ID in sequence"""
        file_sequence = self.get_file_sequence()
        if current_file_id in file_sequence:
            current_index = file_sequence.index(current_file_id)
            if current_index + 1 < len(file_sequence):
                return file_sequence[current_index + 1]
        return None
    
    def get_previous_file(self, current_file_id: int) -> Optional[int]:
        """Get previous file ID in sequence"""
        file_sequence = self.get_file_sequence()
        if current_file_id in file_sequence:
            current_index = file_sequence.index(current_file_id)
            if current_index > 0:
                return file_sequence[current_index - 1]
        return None

class CacheManager:
    """Main cache management system"""
    
    def __init__(self):
        self.config = CacheConfiguration()
        self.status = CacheStatus()
        self.file_manager = FileSequenceManager()
        self.executor = ThreadPoolExecutor(max_workers=self.config.get('performance.max_workers', 4))
        self.generation_lock = threading.Lock()
        self.running = False
        
        logger.info("Cache Manager initialized")
        logger.info(f"Cache window: Nr={self.config.get('cache_window.Nr')}, Nf={self.config.get('cache_window.Nf')}")
    
    def start(self):
        """Start the cache manager"""
        self.running = True
        logger.info("Cache Manager started")
    
    def stop(self):
        """Stop the cache manager"""
        self.running = False
        self.executor.shutdown(wait=True)
        logger.info("Cache Manager stopped")
    
    def update_current_file(self, file_id: int):
        """Update current file and trigger cache generation"""
        logger.info(f"Updating current file to {file_id}")
        
        self.status.update_current_file(file_id)
        
        if self.config.get('enable_background_generation', True):
            self._trigger_background_generation(file_id)
    
    def _trigger_background_generation(self, current_file_id: int):
        """Trigger background generation for cache window"""
        Nr, Nf = self.config.get_cache_window()
        cache_targets = self.file_manager.get_cache_targets(current_file_id, Nr, Nf)
        
        # Filter targets that need caching
        targets_to_generate = []
        for file_id in cache_targets:
            needs_generation = False
            for cache_type in ['segments', 'plots', 'verification']:
                if (self.config.get(f'cache_types.{cache_type}', True) and 
                    not self.status.is_cached(file_id, cache_type)):
                    needs_generation = True
                    break
            
            if needs_generation:
                targets_to_generate.append(file_id)
        
        if targets_to_generate:
            logger.info(f"Queuing {len(targets_to_generate)} files for background generation")
            self.status.add_to_queue(targets_to_generate)
            
            # Submit generation tasks
            for file_id in targets_to_generate:
                self.executor.submit(self._generate_cache_for_file, file_id)
    
    def _generate_cache_for_file(self, file_id: int):
        """Generate all cache types for a specific file"""
        logger.info(f"Starting cache generation for file {file_id}")
        
        try:
            # Generate segments cache
            if self.config.get('cache_types.segments', True):
                self._generate_segments_cache(file_id)
            
            # Generate plots cache
            if self.config.get('cache_types.plots', True):
                self._generate_plots_cache(file_id)
            
            # Generate verification cache
            if self.config.get('cache_types.verification', True):
                self._generate_verification_cache(file_id)
            
            logger.info(f"Completed cache generation for file {file_id}")
            
        except Exception as e:
            logger.error(f"Error generating cache for file {file_id}: {e}")
    
    def _generate_segments_cache(self, file_id: int):
        """Generate segments cache for file"""
        start_time = time.time()
        
        try:
            self.status.mark_in_progress(file_id, 'segments')
            
            # Import here to avoid circular imports
            from data_segment_visualizer_20250531_140000_0_0_1_1 import generate_segments_for_file
            
            # Generate segments
            segments = generate_segments_for_file(file_id)
            
            # Cache the segments data
            cache_file = os.path.join(SEGMENTS_CACHE_DIR, f"segments_{file_id:08d}.json")
            with open(cache_file, 'w') as f:
                json.dump(segments, f, indent=2)
            
            generation_time = time.time() - start_time
            self.status.mark_completed(file_id, 'segments', generation_time)
            
            logger.info(f"Generated segments cache for file {file_id} in {generation_time:.2f}s")
            
        except Exception as e:
            self.status.mark_failed(file_id, 'segments', str(e))
            logger.error(f"Failed to generate segments cache for file {file_id}: {e}")
    
    def _generate_plots_cache(self, file_id: int):
        """Generate plots cache for file"""
        start_time = time.time()
        
        try:
            self.status.mark_in_progress(file_id, 'plots')
            
            # Import here to avoid circular imports
            from data_segment_visualizer_20250531_140000_0_0_1_1 import SegmentPlotter
            
            # Load segments from cache or generate
            segments_cache_file = os.path.join(SEGMENTS_CACHE_DIR, f"segments_{file_id:08d}.json")
            if os.path.exists(segments_cache_file):
                with open(segments_cache_file, 'r') as f:
                    segments = json.load(f)
            else:
                from data_segment_visualizer_20250531_140000_0_0_1_1 import generate_segments_for_file
                segments = generate_segments_for_file(file_id)
            
            # Group segments by length
            segments_by_length = {}
            for segment in segments:
                length = segment['segment_length']
                if length not in segments_by_length:
                    segments_by_length[length] = []
                segments_by_length[length].append(segment)
            
            # Generate plots
            plotter = SegmentPlotter(PLOTS_CACHE_DIR)
            plot_files = plotter.create_time_series_rectangle_plots(file_id, segments_by_length)
            
            # Cache plot metadata
            cache_file = os.path.join(PLOTS_CACHE_DIR, f"plots_meta_{file_id:08d}.json")
            with open(cache_file, 'w') as f:
                json.dump(plot_files, f, indent=2)
            
            generation_time = time.time() - start_time
            self.status.mark_completed(file_id, 'plots', generation_time)
            
            logger.info(f"Generated plots cache for file {file_id} in {generation_time:.2f}s")
            
        except Exception as e:
            self.status.mark_failed(file_id, 'plots', str(e))
            logger.error(f"Failed to generate plots cache for file {file_id}: {e}")
    
    def _generate_verification_cache(self, file_id: int):
        """Generate verification cache for file"""
        start_time = time.time()
        
        try:
            self.status.mark_in_progress(file_id, 'verification')
            
            # For now, just mark as completed since verification is lightweight
            # In future, could pre-generate verification plots
            
            generation_time = time.time() - start_time
            self.status.mark_completed(file_id, 'verification', generation_time)
            
            logger.info(f"Generated verification cache for file {file_id} in {generation_time:.2f}s")
            
        except Exception as e:
            self.status.mark_failed(file_id, 'verification', str(e))
            logger.error(f"Failed to generate verification cache for file {file_id}: {e}")
    
    def get_cached_segments(self, file_id: int) -> Optional[List]:
        """Get cached segments for file"""
        cache_file = os.path.join(SEGMENTS_CACHE_DIR, f"segments_{file_id:08d}.json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r') as f:
                    self.status.status["stats"]["cache_hits"] += 1
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading cached segments for file {file_id}: {e}")
        
        self.status.status["stats"]["cache_misses"] += 1
        return None
    
    def get_cached_plots(self, file_id: int) -> Optional[Dict]:
        """Get cached plot metadata for file"""
        cache_file = os.path.join(PLOTS_CACHE_DIR, f"plots_meta_{file_id:08d}.json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading cached plots for file {file_id}: {e}")
        return None
    
    def cleanup_cache(self):
        """Clean up old cache files"""
        logger.info("Starting cache cleanup")
        
        max_age_hours = self.config.get('cache_limits.max_cache_age_hours', 24)
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        
        for cache_dir in [SEGMENTS_CACHE_DIR, PLOTS_CACHE_DIR, VERIFICATION_CACHE_DIR]:
            for filename in os.listdir(cache_dir):
                file_path = os.path.join(cache_dir, filename)
                if os.path.isfile(file_path):
                    file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if file_time < cutoff_time:
                        try:
                            os.remove(file_path)
                            logger.debug(f"Removed old cache file: {filename}")
                        except Exception as e:
                            logger.error(f"Error removing cache file {filename}: {e}")
        
        logger.info("Cache cleanup completed")
    
    def get_status_summary(self) -> Dict:
        """Get comprehensive cache status summary"""
        return {
            "config": self.config.config,
            "status": self.status.status,
            "queue_status": self.status.get_queue_status(),
            "cache_dirs": {
                "segments": len(os.listdir(SEGMENTS_CACHE_DIR)),
                "plots": len(os.listdir(PLOTS_CACHE_DIR)),
                "verification": len(os.listdir(VERIFICATION_CACHE_DIR))
            }
        }

# Global cache manager instance
cache_manager = None

def get_cache_manager() -> CacheManager:
    """Get global cache manager instance"""
    global cache_manager
    if cache_manager is None:
        cache_manager = CacheManager()
        cache_manager.start()
    return cache_manager

if __name__ == '__main__':
    # Test the cache manager
    cache_mgr = get_cache_manager()
    
    print("=== CACHE MANAGER TEST ===")
    print(f"Configuration: {cache_mgr.config.config}")
    
    # Test with a file
    test_file_id = 10
    print(f"\nTesting cache generation for file {test_file_id}")
    cache_mgr.update_current_file(test_file_id)
    
    # Wait a bit for background generation
    time.sleep(5)
    
    print(f"\nCache status summary:")
    summary = cache_mgr.get_status_summary()
    print(json.dumps(summary, indent=2))
    
    cache_mgr.stop()