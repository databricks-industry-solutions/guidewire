#!/usr/bin/env python3
"""
Generate larger example datasets for Guidewire CDA Delta Clone testing.
Creates realistic policy holder data with varying transaction volumes.

This script creates data independently in a separate directory structure
without affecting existing examples or manifests.
"""

import pandas as pd
import numpy as np
import os
import uuid
import random
import argparse
import json
from datetime import datetime, timedelta
from faker import Faker

BUCKET_NAME = "sumanmisra"

# Initialize Faker for realistic data generation
fake = Faker()

def set_random_seed(seed=None):
    """Set random seeds for reproducible results."""
    if seed is None:
        seed = random.randint(1, 10000)
    
    fake.seed_instance(seed)
    np.random.seed(seed)
    random.seed(seed)
    print(f"Using random seed: {seed}")
    return seed

def generate_policy_holder_data(num_records, include_lastname=True, base_timestamp=None):
    """Generate policy holder data with specified number of records."""
    
    data = {
        'firstName': [fake.first_name() for _ in range(num_records)],
        'age': np.random.randint(18, 85, num_records)
    }
    
    if include_lastname:
        data['lastName'] = [fake.last_name() for _ in range(num_records)]
    
    # Add some additional realistic fields for larger datasets
    if num_records > 100:
        data.update({
            'policyNumber': [f'POL-{fake.random_int(100000, 999999)}' for _ in range(num_records)],
            'premium': np.round(np.random.uniform(500, 5000, num_records), 2),
            'state': [fake.state_abbr() for _ in range(num_records)],
            'zipCode': [fake.zipcode() for _ in range(num_records)],
            'vehicleYear': np.random.randint(2000, 2024, num_records),
            'vehicleMake': [random.choice(['Toyota', 'Honda', 'Ford', 'Chevrolet', 'BMW', 'Mercedes', 'Audi', 'Nissan', 'Hyundai', 'Kia']) for _ in range(num_records)],
            'coverageType': [random.choice(['Full Coverage', 'Liability Only', 'Comprehensive', 'Collision']) for _ in range(num_records)],
            'riskScore': np.round(np.random.uniform(1.0, 10.0, num_records), 2)
        })
    
    return pd.DataFrame(data)

def generate_claim_data(num_records, include_lastname=True, base_timestamp=None):
    """Generate claim data with specified number of records."""
    
    data = {
        'claimNumber': [f'CLM-{fake.random_int(100000, 999999)}' for _ in range(num_records)],
        'policyNumber': [f'POL-{fake.random_int(100000, 999999)}' for _ in range(num_records)],
        'claimAmount': np.round(np.random.uniform(100, 50000, num_records), 2),
        'claimStatus': [random.choice(['Open', 'Closed', 'Pending', 'Under Investigation']) for _ in range(num_records)]
    }
    
    if include_lastname:
        data.update({
            'claimantFirstName': [fake.first_name() for _ in range(num_records)],
            'claimantLastName': [fake.last_name() for _ in range(num_records)]
        })
    else:
        data['claimantFirstName'] = [fake.first_name() for _ in range(num_records)]
    
    # Add additional fields for larger datasets
    if num_records > 100:
        data.update({
            'incidentDate': [fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d') for _ in range(num_records)],
            'reportedDate': [fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d') for _ in range(num_records)],
            'claimType': [random.choice(['Collision', 'Comprehensive', 'Liability', 'Medical', 'Property']) for _ in range(num_records)],
            'adjusterId': [f'ADJ-{fake.random_int(1000, 9999)}' for _ in range(num_records)],
            'severity': [random.choice(['Minor', 'Moderate', 'Major', 'Total Loss']) for _ in range(num_records)],
            'estimatedAmount': np.round(np.random.uniform(50, 45000, num_records), 2),
            'state': [fake.state_abbr() for _ in range(num_records)]
        })
    
    return pd.DataFrame(data)

def generate_invoicing_data(num_records, include_lastname=True, base_timestamp=None):
    """Generate invoicing data with specified number of records."""
    
    data = {
        'invoiceNumber': [f'INV-{fake.random_int(100000, 999999)}' for _ in range(num_records)],
        'policyNumber': [f'POL-{fake.random_int(100000, 999999)}' for _ in range(num_records)],
        'invoiceAmount': np.round(np.random.uniform(200, 3000, num_records), 2),
        'invoiceStatus': [random.choice(['Paid', 'Pending', 'Overdue', 'Cancelled']) for _ in range(num_records)]
    }
    
    if include_lastname:
        data.update({
            'customerFirstName': [fake.first_name() for _ in range(num_records)],
            'customerLastName': [fake.last_name() for _ in range(num_records)]
        })
    else:
        data['customerFirstName'] = [fake.first_name() for _ in range(num_records)]
    
    # Add additional fields for larger datasets
    if num_records > 100:
        data.update({
            'invoiceDate': [fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d') for _ in range(num_records)],
            'dueDate': [fake.date_between(start_date='today', end_date='+90d').strftime('%Y-%m-%d') for _ in range(num_records)],
            'paymentMethod': [random.choice(['Credit Card', 'Bank Transfer', 'Check', 'Auto Pay']) for _ in range(num_records)],
            'billingPeriod': [random.choice(['Monthly', 'Quarterly', 'Semi-Annual', 'Annual']) for _ in range(num_records)],
            'agentId': [f'AGT-{fake.random_int(1000, 9999)}' for _ in range(num_records)],
            'taxAmount': np.round(np.random.uniform(10, 300, num_records), 2),
            'discountAmount': np.round(np.random.uniform(0, 200, num_records), 2)
        })
    
    return pd.DataFrame(data)

def generate_table_data(table_type, num_records, include_lastname=True, base_timestamp=None):
    """Generate data based on table type."""
    if table_type == 'policy_holders':
        return generate_policy_holder_data(num_records, include_lastname, base_timestamp)
    elif table_type == 'claim':
        return generate_claim_data(num_records, include_lastname, base_timestamp)
    elif table_type == 'invoicing':
        return generate_invoicing_data(num_records, include_lastname, base_timestamp)
    else:
        raise ValueError(f"Unsupported table type: {table_type}")

def create_parquet_files(entity_id, timestamp_dir, num_files, records_per_file, table_type="policy_holders", include_lastname=True, output_dir="generated_data"):
    """Create multiple parquet files for a given entity and timestamp."""
    
    entity_path = f"{output_dir}/cda/{table_type}/{entity_id}/{timestamp_dir}"
    os.makedirs(entity_path, exist_ok=True)
    
    total_records = 0
    
    for i in range(num_files):
        # Generate unique filename similar to existing format
        file_uuid = str(uuid.uuid4())
        filename = f"part-{i:05d}-{file_uuid}-c000.snappy.parquet"
        filepath = os.path.join(entity_path, filename)
        
        # Generate data based on table type
        df = generate_table_data(table_type, records_per_file, include_lastname)
        
        # Save as parquet
        df.to_parquet(filepath, compression='snappy', index=False)
        total_records += len(df)
        
        print(f"Created {filepath} with {len(df)} records")
    
    return total_records

def get_default_scenarios():
    """Get default data generation scenarios for all table types."""
    return [
        # Policy Holders - Small datasets
        {
            'table_type': 'policy_holders',
            'entity_id': '401000001',
            'timestamps': [
                ('1683000000000', 2, 100, True),   # 2 files, 100 records each
                ('1683100000000', 3, 150, True),   # 3 files, 150 records each
            ]
        },
        # Policy Holders - Medium datasets
        {
            'table_type': 'policy_holders',
            'entity_id': '401000002', 
            'timestamps': [
                ('1683200000000', 5, 800, True),   # 5 files, 800 records each
                ('1683300000000', 8, 1200, True),  # 8 files, 1200 records each
            ]
        },
        # Claims - Small datasets
        {
            'table_type': 'claim',
            'entity_id': '501000001',
            'timestamps': [
                ('1683000000000', 3, 200, True),   # 3 files, 200 records each
                ('1683100000000', 4, 250, True),   # 4 files, 250 records each
            ]
        },
        # Claims - Medium datasets
        {
            'table_type': 'claim',
            'entity_id': '501000002',
            'timestamps': [
                ('1683200000000', 6, 1000, True),  # 6 files, 1000 records each
                ('1683300000000', 8, 1500, True),  # 8 files, 1500 records each
            ]
        },
        # Invoicing - Small datasets
        {
            'table_type': 'invoicing',
            'entity_id': '601000001',
            'timestamps': [
                ('1683000000000', 2, 300, True),   # 2 files, 300 records each
                ('1683100000000', 4, 400, True),   # 4 files, 400 records each
            ]
        },
        # Invoicing - Medium datasets
        {
            'table_type': 'invoicing',
            'entity_id': '601000002',
            'timestamps': [
                ('1683200000000', 7, 1200, True),  # 7 files, 1200 records each
                ('1683300000000', 10, 1800, True), # 10 files, 1800 records each
            ]
        },
        # Large datasets - Policy Holders
        {
            'table_type': 'policy_holders',
            'entity_id': '401000003',
            'timestamps': [
                ('1683400000000', 12, 2500, True),  # 12 files, 2500 records each (30k total)
                ('1683500000000', 15, 3000, True),  # 15 files, 3000 records each (45k total)
            ]
        },
        # Large datasets - Claims
        {
            'table_type': 'claim',
            'entity_id': '501000003',
            'timestamps': [
                ('1683400000000', 10, 2000, True),  # 10 files, 2000 records each (20k total)
                ('1683500000000', 12, 2500, True),  # 12 files, 2500 records each (30k total)
            ]
        },
        # Mixed schema examples (some with lastName/customer names, some without)
        {
            'table_type': 'policy_holders',
            'entity_id': '401000004',
            'timestamps': [
                ('1683600000000', 4, 1000, False),  # No lastName field
                ('1683700000000', 6, 1500, True),   # With lastName field
            ]
        },
        {
            'table_type': 'claim',
            'entity_id': '501000004',
            'timestamps': [
                ('1683600000000', 3, 800, False),   # No claimant lastName
                ('1683700000000', 5, 1200, True),   # With claimant lastName
            ]
        },
        # Many timestamps scenario - 1000 timestamp folders with minimal files each
        {
            'table_type': 'policy_holders',
            'entity_id': '401000005',
            'timestamps': [
                # Generate 1000 timestamps, each with 1 file containing 10 records
                (str(1684000000000 + i * 86400000), 1, 10, True) for i in range(568)
            ]
        },
        {
            'table_type': 'claim',
            'entity_id': '401000005',
            'timestamps': [
                # Generate 1000 timestamps, each with 1 file containing 10 records
                (str(1684000000000 + i * 86400000), 1, 10, True) for i in range(786)
            ]
        },
        {
            'table_type': 'invoicing',
            'entity_id': '401000005',   
            'timestamps': [
                # Generate 1000 timestamps, each with 1 file containing 10 records
                (str(1684000000000 + i * 86400000), 1, 10, True) for i in range(236)
            ]
        }
    ]

def create_manifest(entity_data, output_dir):
    """Create a new manifest file for the generated data."""
    manifest = {}
    
    for (table_type, entity_id), data in entity_data.items():
        entity_key = f"{table_type}_{entity_id}"
        manifest[entity_key] = {
            "lastSuccessfulWriteTimestamp": data['latest_timestamp'],
            "totalProcessedRecordsCount": data['total_records'],
            "dataFilesPath": f"s3://{BUCKET_NAME}cda/{table_type}/",
            "schemaHistory": {
                entity_id: data['latest_timestamp']
            }
        }
    
    manifest_path = f"{output_dir}/cda/manifest.json"
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    return manifest_path

def main():
    """Generate comprehensive example datasets independently."""
    
    parser = argparse.ArgumentParser(description='Generate example data for Guidewire CDA testing')
    parser.add_argument('--output-dir', '-o', default='generated_data', 
                       help='Output directory for generated data (default: generated_data)')
    parser.add_argument('--seed', '-s', type=int, default=None,
                       help='Random seed for reproducible results')
    parser.add_argument('--scenarios', '-c', choices=['small', 'medium', 'large', 'all'], 
                       default='all', help='Which scenarios to generate')
    parser.add_argument('--table-types', '-t', nargs='+', 
                       choices=['policy_holders', 'claim', 'invoicing', 'all'],
                       default=['all'], help='Which table types to generate')
    parser.add_argument('--custom-entity', '-e', type=str,
                       help='Generate single custom entity (format: table_type:entity_id:files:records_per_file)')
    
    args = parser.parse_args()
    
    # Set random seed
    seed = set_random_seed(args.seed)
    
    print(f"Generating example datasets independently...")
    print(f"Output directory: {args.output_dir}")
    print(f"Random seed: {seed}")
    
    # Handle custom entity generation
    if args.custom_entity:
        try:
            parts = args.custom_entity.split(':')
            table_type = parts[0]
            entity_id = parts[1]
            num_files = int(parts[2])
            records_per_file = int(parts[3])
            
            if table_type not in ['policy_holders', 'claim', 'invoicing']:
                raise ValueError(f"Invalid table type: {table_type}")
            
            timestamp = str(int(datetime.now().timestamp() * 1000))
            
            scenarios = [{
                'table_type': table_type,
                'entity_id': entity_id,
                'timestamps': [(timestamp, num_files, records_per_file, True)]
            }]
            print(f"Generating custom entity: {table_type}.{entity_id} ({num_files} files × {records_per_file} records)")
            
        except (IndexError, ValueError) as e:
            print("Error: Custom entity format should be 'table_type:entity_id:files:records_per_file'")
            print("Example: --custom-entity policy_holders:999000001:5:1000")
            print(f"Details: {e}")
            return
    else:
        # Get default scenarios
        all_scenarios = get_default_scenarios()
        
        # Filter by table types
        if 'all' not in args.table_types:
            all_scenarios = [s for s in all_scenarios if s['table_type'] in args.table_types]
        
        # Filter by scenario size
        if args.scenarios == 'small':
            scenarios = [s for s in all_scenarios if '001' in s['entity_id']]  # Small entity IDs end in 001
        elif args.scenarios == 'medium':
            scenarios = [s for s in all_scenarios if '002' in s['entity_id']]  # Medium entity IDs end in 002
        elif args.scenarios == 'large':
            scenarios = [s for s in all_scenarios if '003' in s['entity_id']]  # Large entity IDs end in 003
        else:  # all
            scenarios = all_scenarios
    
    # Track totals for manifest creation - now keyed by (table_type, entity_id)
    entity_data = {}
    
    # Generate data for each scenario
    for scenario in scenarios:
        table_type = scenario['table_type']
        entity_id = scenario['entity_id']
        entity_total = 0
        key = (table_type, entity_id)
        
        print(f"\nGenerating data for {table_type}.{entity_id}:")
        
        for timestamp, num_files, records_per_file, include_lastname in scenario['timestamps']:
            print(f"  Timestamp {timestamp}: {num_files} files × {records_per_file} records")
            
            total_records = create_parquet_files(
                entity_id, timestamp, num_files, records_per_file, 
                table_type, include_lastname, args.output_dir
            )
            
            entity_total += total_records
            latest_timestamp = timestamp
        
        entity_data[key] = {
            'total_records': entity_total,
            'latest_timestamp': latest_timestamp
        }
        print(f"  Total records for {table_type}.{entity_id}: {entity_total:,}")
    
    # Create independent manifest
    print(f"\nCreating independent manifest...")
    manifest_path = create_manifest(entity_data, args.output_dir)
    print(f"Created {manifest_path}")
    
    # Print summary
    print(f"\n{'='*60}")
    print("GENERATION COMPLETE")
    print(f"{'='*60}")
    
    total_all_records = sum(data['total_records'] for data in entity_data.values())
    print(f"Total records generated: {total_all_records:,}")
    print(f"Entities created: {len(entity_data)}")
    
    # Group by table type for summary
    table_summaries = {}
    for (table_type, entity_id), data in entity_data.items():
        if table_type not in table_summaries:
            table_summaries[table_type] = {'entities': 0, 'records': 0}
        table_summaries[table_type]['entities'] += 1
        table_summaries[table_type]['records'] += data['total_records']
    
    for table_type, summary in table_summaries.items():
        print(f"  {table_type}: {summary['entities']} entities, {summary['records']:,} records")
    
    print(f"\nData files created in: {args.output_dir}/cda/")
    print(f"Manifest created: {manifest_path}")
    print(f"\nTo use this data with the main application:")
    print(f"  1. Copy/move {args.output_dir}/cda/ to your desired location")
    print(f"  2. Update your application config to point to the new data location")

if __name__ == "__main__":
    main()
