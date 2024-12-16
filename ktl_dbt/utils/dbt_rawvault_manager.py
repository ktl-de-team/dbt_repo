# dbt_manager.py
import argparse
from pathlib import Path
import logging
import yaml
import shutil
from datetime import datetime
import sys
from typing import Dict, List, Tuple

def get_default_paths() -> Dict[str, str]:
    """
    Get default paths based on dbt_manager.py location
    
    Returns:
        Dict containing default base_config and base_models paths
    """
    # Get current script location
    current_path = Path(__file__).resolve()
    
    # Navigate up to ktl_dbt root
    # From: .../ktl_dbt/ktl_autovault_configs/utils/dbt_manager.py
    # To: .../ktl_dbt
    project_root = current_path.parents[3]  # Go up 3 levels
    
    # Construct default paths
    default_paths = {
        'base_config': str(project_root / 'ktl_autovault_configs'),
        'base_models': str(project_root / 'models' / 'integration_dp' / 'raw_vault')
    }
    
    return default_paths

class DbtProjectManager:
    def __init__(self, config_file: str = "config_rawvault.yml"):
        self.logger = self._setup_logging()
        self.config = self._load_config(config_file)
        # Set up paths
        self.base_config_path = Path(self.config['paths']['base_config'])
        self.base_models_path = Path(self.config['paths']['base_models'])
        
        # Construct stream and batch paths by joining with base_models
        self.stream_models_path = self.base_models_path / self.config['paths']['stream']
        self.batch_models_path = self.base_models_path / self.config['paths']['batch']

    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

    def _load_config(self, config_file: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_file, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.error(f"Error loading config file: {e}")
            sys.exit(1)

    def check_vault_structure(self) -> Dict[str, bool]:
        """Check the entire vault structure"""
        expected_structure = {
            'batch': ['lsat', 'sat'],
            'stream': ['hub', 'lnk', 'lsat', 'sat']
        }
        
        structure_status = {
            'config_exists': self.base_config_path.exists(),
            'stream_exists': self.stream_models_path.exists(),
            'batch_exists': self.batch_models_path.exists()
        }
        
        # Check config subdirectories
        for subdir in ['hub', 'lnk', 'sat', 'lsat']:
            config_subdir = self.base_config_path / subdir
            structure_status[f'config_{subdir}'] = config_subdir.exists()

        # Check model subdirectories
        for main_dir, subdirs in expected_structure.items():
            base_path = self.stream_models_path if main_dir == 'stream' else self.batch_models_path
            for subdir in subdirs:
                structure_status[f'{main_dir}_{subdir}'] = (base_path / subdir).exists()

        return structure_status

    def create_missing_directories(self, structure_status: Dict[str, bool]) -> List[str]:
        """Create any missing directories"""
        created_dirs = []
        expected_structure = {
            'batch': ['lsat', 'sat'],
            'stream': ['hub', 'lnk', 'lsat', 'sat']
        }

        # Create base directories
        for path in [self.base_config_path, self.stream_models_path, self.batch_models_path]:
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)
                created_dirs.append(str(path))

        # Create config subdirectories
        for subdir in ['hub', 'lnk', 'sat', 'lsat']:
            dir_path = self.base_config_path / subdir
            if not dir_path.exists():
                dir_path.mkdir(parents=True, exist_ok=True)
                created_dirs.append(str(dir_path))

        # Create model subdirectories
        for main_dir, subdirs in expected_structure.items():
            base_path = self.stream_models_path if main_dir == 'stream' else self.batch_models_path
            for subdir in subdirs:
                dir_path = base_path / subdir
                if not dir_path.exists():
                    dir_path.mkdir(parents=True, exist_ok=True)
                    created_dirs.append(str(dir_path))

        return created_dirs

    def generate_sql_files(self, processing_type: str = 'all') -> Dict[str, Dict[str, bool]]:
        """Generate SQL files for stream and/or batch processing"""
        results = {}
        
        if processing_type in ['all', 'stream']:
            results['stream'] = self._generate_stream_files()
            
        if processing_type in ['all', 'batch']:
            results['batch'] = self._generate_batch_files()
            
        return results

    def _generate_stream_files(self) -> Dict[str, bool]:
        """Generate stream processing SQL files"""
        results = {}
        for table_type in ['hub', 'lnk', 'sat', 'lsat']:
            yaml_dir = self.base_config_path / table_type
            if not yaml_dir.exists():
                continue

            for yaml_file in yaml_dir.glob("*.yml"):
                if table_type in ['sat', 'lsat']:
                    sql_dir = self.stream_models_path / table_type
                    # Generate correct der filename (table_type_der_name_indi.sql)
                    sql_filename = f"{table_type}_der_{yaml_file.stem.replace(f'{table_type}_', '')}.sql"
                    template_key = f"{table_type}_der"
                else:
                    sql_dir = self.stream_models_path / table_type
                    sql_filename = f"{yaml_file.stem}.sql"
                    template_key = table_type

                sql_dir.mkdir(parents=True, exist_ok=True)
                sql_path = sql_dir / sql_filename
                
                try:
                    template = self.config['templates'][template_key]
                    sql_content = template.replace('{{config_name}}', yaml_file.stem)
                    with sql_path.open('w') as f:
                        f.write(sql_content)
                    results[sql_filename] = True
                except Exception as e:
                    self.logger.error(f"Error generating {sql_filename}: {e}")
                    results[sql_filename] = False
        return results
    
    def _generate_batch_files(self) -> Dict[str, bool]:
        """Generate batch processing SQL files"""
        results = {}
        for table_type in ['sat', 'lsat']:
            yaml_dir = self.base_config_path / table_type
            if not yaml_dir.exists():
                continue

            for yaml_file in yaml_dir.glob("*.yml"):
                sql_dir = self.batch_models_path / table_type
                sql_dir.mkdir(parents=True, exist_ok=True)

                # Standard file - keep original name
                standard_filename = f"{yaml_file.stem}.sql"
                standard_path = sql_dir / standard_filename
                try:
                    template = self.config['templates'][table_type]
                    sql_content = template.replace('{{config_name}}', yaml_file.stem)
                    with standard_path.open('w') as f:
                        f.write(sql_content)
                    results[standard_filename] = True
                except Exception as e:
                    self.logger.error(f"Error generating {standard_filename}: {e}")
                    results[standard_filename] = False

                # Snapshot file - using correct table_type_snp format
                snp_filename = f"{table_type}_snp_{yaml_file.stem.replace(f'{table_type}_', '')}.sql"
                snp_path = sql_dir / snp_filename
                try:
                    template = self.config['templates'][f"{table_type}_snp"]
                    sql_content = template.replace('{{config_name}}', yaml_file.stem)
                    with snp_path.open('w') as f:
                        f.write(sql_content)
                    results[snp_filename] = True
                except Exception as e:
                    self.logger.error(f"Error generating {snp_filename}: {e}")
                    results[snp_filename] = False

        return results
    
    def generate_config_file(self, base_config: str, base_models: str, output_file: str = "config_rawvault.yml") -> bool:
        """
        Generate config_rawvault.yml file with provided paths
        
        Args:
            base_config: Path to ktl_autovault_configs directory
            base_models: Path to raw_vault directory
            output_file: Output config file name
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            config_content = {
                'paths': {
                    'base_config': base_config,
                    'base_models': base_models,
                    'stream': 'stream',
                    'batch': 'batch'
                },
                'templates': {
                    'hub': """{%- set {{config_name}} = dv_config('{{config_name}}') -%}
    {%- set dv_system = var("dv_system") -%}

    {{ ktl_autovault.hub_transform(model={{config_name}}, dv_system=dv_system, include_ghost_record=true) }}""",
                    
                    'lnk': """{%- set {{config_name}} = dv_config('{{config_name}}') -%}
    {%- set dv_system = var("dv_system") -%}

    {{ ktl_autovault.lnk_transform(model={{config_name}}, dv_system=dv_system) }}""",
                    
                    'sat_der': """{%- set {{config_name}} = dv_config('{{config_name}}') -%}
    {%- set dv_system = var("dv_system") -%}

    {{ ktl_autovault.sat_der_transform(model={{config_name}}, dv_system=dv_system) }}""",
                    
                    'lsat_der': """{%- set {{config_name}} = dv_config('{{config_name}}') -%}
    {%- set dv_system = var("dv_system") -%}

    {{ ktl_autovault.lsat_der_transform(model={{config_name}}, dv_system=dv_system) }}""",
                    
                    'sat_snp': """{%- set {{config_name}} = dv_config('{{config_name}}') -%}
    {%- set dv_system = var("dv_system") -%}

    {{ ktl_autovault.sat_snp_transform(model={{config_name}}, dv_system=dv_system) }}""",
                    
                    'lsat_snp': """{%- set {{config_name}} = dv_config('{{config_name}}') -%}
    {%- set dv_system = var("dv_system") -%}

    {{ ktl_autovault.lsat_snp_transform(model={{config_name}}, dv_system=dv_system) }}""",
                    
                    'sat': """{%- set {{config_name}} = dv_config('{{config_name}}') -%}
    {%- set dv_system = var("dv_system") -%}

    {{ ktl_autovault.sat_transform(model={{config_name}}, dv_system=dv_system) }}""",
                    
                    'lsat': """{%- set {{config_name}} = dv_config('{{config_name}}') -%}
    {%- set dv_system = var("dv_system") -%}

    {{ ktl_autovault.lsat_transform(model={{config_name}}, dv_system=dv_system) }}"""
                }
            }

            # Write config file with proper YAML formatting
            with open(output_file, 'w') as f:
                yaml.dump(config_content, f, default_flow_style=False, sort_keys=False)
            
            self.logger.info(f"Successfully generated config file: {output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error generating config file: {e}")
            return False
        
def main():
    default_paths=get_default_paths()
    parser = argparse.ArgumentParser(description='DBT Project Manager')
    parser.add_argument('action', choices=['check', 'create', 'generate', 'config'], 
                      help='Action to perform (check structure, create directories, generate SQL, or create config)')
    parser.add_argument('--type', choices=['stream', 'batch', 'all'], default='all',
                      help='Processing type for SQL generation (default: all)')
    parser.add_argument('--config', default='config_rawvault.yml',
                      help='Path to configuration file (default: config_rawvault.yml)')
    parser.add_argument('--base-config',
                      default=default_paths['base_config'],
                      help=f'Base config path for config generation (default: {default_paths["base_config"]})')
    parser.add_argument('--base-models',
                      default=default_paths['base_models'],
                      help=f'Base models path for config generation (default: {default_paths["base_models"]})')


    args = parser.parse_args()
    
    manager = DbtProjectManager(args.config)
    
    if args.action == 'check':
        status = manager.check_vault_structure()
        for key, exists in status.items():
            symbol = "✓" if exists else "✗"
            print(f"{symbol} {key}")
        return 0 if all(status.values()) else 1
        
    elif args.action == 'create':
        status = manager.check_vault_structure()
        created = manager.create_missing_directories(status)
        if created:
            print("Created directories:")
            for dir_path in created:
                print(f"  + {dir_path}")
        else:
            print("No directories needed to be created")
        return 0
        
    elif args.action == 'generate':
        results = manager.generate_sql_files(args.type)
        success = True
        for proc_type, files in results.items():
            print(f"\n{proc_type.upper()} Files:")
            for filename, status in files.items():
                symbol = "✓" if status else "✗"
                print(f"  {symbol} {filename}")
                success = success and status
        return 0 if success else 1
    
    elif args.action == 'config':
        if not args.base_config or not args.base_models:
            print("Error: --base-config and --base-models are required for config generation")
            return 1
        success = manager.generate_config_file(args.base_config, args.base_models)
        return 0 if success else 1
    
if __name__ == "__main__":
    sys.exit(main())