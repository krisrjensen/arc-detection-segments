#!/usr/bin/env python3
"""
Universal Image Save Functionality for Transient Service
Provides consistent image saving capabilities across all services
"""

import os
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
import json

class UniversalImageSaver:
    """Universal image saving system with format support and metadata"""
    
    def __init__(self, base_dir=None):
        self.base_dir = base_dir or "/tmp/plots"
        self.supported_formats = ['png', 'pdf', 'svg', 'eps', 'jpg']
        self.default_format = 'png'
        self.default_dpi = 150
        
        # Ensure base directory exists
        os.makedirs(self.base_dir, exist_ok=True)
    
    def save_plot(self, filename, experiment_name=None, style='default', 
                  formats=None, dpi=None, bbox_inches='tight', 
                  metadata=None):
        """
        Save plot with universal format support and metadata
        
        Args:
            filename: Base filename (without extension)
            experiment_name: Name of experiment for organization
            style: Style name for consistent naming
            formats: List of formats to save (default: ['png'])
            dpi: Resolution for raster formats
            bbox_inches: Bounding box setting
            metadata: Dictionary of metadata to save
        
        Returns:
            Dictionary with saved file paths and metadata
        """
        if formats is None:
            formats = [self.default_format]
        
        if dpi is None:
            dpi = self.default_dpi
        
        # Create experiment subdirectory if specified
        save_dir = self.base_dir
        if experiment_name:
            save_dir = os.path.join(self.base_dir, experiment_name)
            os.makedirs(save_dir, exist_ok=True)
        
        # Generate timestamp for versioning
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Prepare metadata
        save_metadata = {
            'timestamp': timestamp,
            'experiment_name': experiment_name,
            'style': style,
            'dpi': dpi,
            'formats': formats,
            'bbox_inches': bbox_inches
        }
        
        if metadata:
            save_metadata.update(metadata)
        
        # Save in each requested format
        saved_files = {}
        for fmt in formats:
            if fmt not in self.supported_formats:
                print(f"Warning: Format {fmt} not supported, skipping")
                continue
            
            # Create filename with timestamp and format
            if experiment_name:
                full_filename = f"{filename}_{timestamp}.{fmt}"
            else:
                full_filename = f"{filename}.{fmt}"
            
            file_path = os.path.join(save_dir, full_filename)
            
            try:
                plt.savefig(file_path, format=fmt, dpi=dpi, 
                           bbox_inches=bbox_inches)
                saved_files[fmt] = file_path
                print(f"Saved {fmt.upper()}: {file_path}")
            except Exception as e:
                print(f"Error saving {fmt} format: {e}")
        
        # Save metadata file
        metadata_filename = f"{filename}_{timestamp}_metadata.json"
        metadata_path = os.path.join(save_dir, metadata_filename)
        
        try:
            with open(metadata_path, 'w') as f:
                json.dump(save_metadata, f, indent=2)
            saved_files['metadata'] = metadata_path
        except Exception as e:
            print(f"Error saving metadata: {e}")
        
        return {
            'saved_files': saved_files,
            'metadata': save_metadata,
            'primary_file': saved_files.get(self.default_format),
            'base_dir': save_dir
        }
    
    def save_transient_plot(self, filename, experiment_path, transient_data, 
                           style='default', formats=None):
        """
        Specialized save function for transient plots with standard metadata
        
        Args:
            filename: Base filename
            experiment_path: Full experiment path
            transient_data: Dictionary with transient detection data
            style: Style name
            formats: Output formats
        
        Returns:
            Save result dictionary
        """
        experiment_name = experiment_path.split('/')[-1] if experiment_path else None
        
        metadata = {
            'plot_type': 'transient_analysis',
            'experiment_path': experiment_path,
            'transient_center': transient_data.get('center_time'),
            'transient_sample': transient_data.get('center_sample'),
            'detection_method': transient_data.get('source', 'unknown'),
            'service': 'transient_analysis'
        }
        
        return self.save_plot(
            filename=filename,
            experiment_name=experiment_name,
            style=style,
            formats=formats,
            metadata=metadata
        )

# Global instance
universal_saver = UniversalImageSaver()