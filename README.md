# Arc Detection Segment Visualization Service

A specialized service for visualizing and analyzing data segments in arc detection experiments. This service provides intelligent caching, segment rectangle visualization, and cache-aware navigation.

## Features

- **Intelligent Caching**: Background pre-generation and instant loading of cached segments
- **Segment Visualization**: Time-series rectangle plots showing data segment boundaries
- **Cache Integration**: Seamless integration with cache manager for performance optimization
- **Styles Gallery**: Consistent styling across visualizations
- **Universal Image Save**: Enhanced image saving with metadata and multiple format support

## Services

### Main Visualizer (`app.py`)
- Port: 5032 (default)
- Cached segment visualization system
- Background pre-generation for navigation window
- Real-time fallback when cache unavailable

### Cache System (`cache/`)
- **Config Server** (`cache/config_server.py`): Cache configuration management
- **Cache Manager** (`cache/manager.py`): Intelligent cache operations and pre-generation

## Quick Start

```bash
# Install dependencies
pip install flask numpy matplotlib pyyaml

# Start the main service
python app.py

# Cache system starts automatically with the main service
```

## Configuration

Key configuration variables:
- `V3_DATABASE_PATH`: Path to V3 database
- `RAW_DATA_DIR`: Raw data directory
- `PLOTS_DIR`: Segment plots output directory
- `DEFAULT_CONFIG`: Segment lengths and overlap settings

Default segment configuration:
```python
{
    "segment_lengths": [524288, 65536, 8192],
    "default_overlap": 0.0,
    "cache_enabled": True
}
```

## API Endpoints

### Main Service
- `GET /`: Segment visualizer interface
- `POST /sync`: Sync with data review tool
- `GET /plot/<filename>`: Serve plot images
- `POST /generate_plot`: Generate segment visualization
- `GET /status`: Service status

### Cache Endpoints
- `GET /cache/status`: Cache system status
- `POST /cache/clear`: Clear cache
- `POST /cache/pregenerate`: Trigger pre-generation

## Cache System

The service includes an intelligent caching system that:
- Pre-generates plots for navigation efficiency
- Provides instant loading for cached segments
- Falls back to real-time generation when needed
- Optimizes performance for large datasets

## Data Segments

Visualizes three levels of data segmentation:
- **Level 1**: 524,288 samples (large segments)
- **Level 2**: 65,536 samples (medium segments)  
- **Level 3**: 8,192 samples (fine segments)

Each level provides different temporal resolution for analysis.

## Version

Current version: `20250602_013500_0_0_1_2`

## Dependencies

- Flask
- NumPy
- Matplotlib
- PyYAML
- SQLite3
- Pathlib

## Integration

This service integrates with:
- Enhanced Data Cleaning Tool (sync protocol)
- V3 Database system
- Cache management system
- Arc Detection main coordination service