"""
Filtering analysis and tracking for Basic Data Fusion.

This module provides functions for tracking filter application,
generating filtering reports, and analyzing filter impact.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from core.database import get_database_manager
from core.exceptions import ValidationError

# Exception alias for this module
DataProcessingError = ValidationError
from data_handling.merge_strategy import MergeKeys
from .demographics import calculate_demographics_breakdown


@dataclass
class FilterStep:
    """Represents a single filter application step."""
    step_number: int
    filter_type: str  # 'demographic' or 'phenotypic'
    filter_description: str
    participant_count_before: int
    participant_count_after: int
    participants_removed: int
    removal_percentage: float
    demographics_before: Dict[str, Any] = field(default_factory=dict)
    demographics_after: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FilterTracker:
    """Tracks filter application sequence and sample size impact."""
    initial_count: int = 0
    current_count: int = 0
    steps: List[FilterStep] = field(default_factory=list)
    
    def add_step(
        self,
        filter_type: str,
        filter_description: str,
        new_count: int,
        demographics_before: Optional[Dict[str, Any]] = None,
        demographics_after: Optional[Dict[str, Any]] = None
    ) -> FilterStep:
        """Add a filter step to the tracker."""
        step_number = len(self.steps) + 1
        participants_removed = self.current_count - new_count
        removal_percentage = (participants_removed / self.current_count * 100) if self.current_count > 0 else 0
        
        step = FilterStep(
            step_number=step_number,
            filter_type=filter_type,
            filter_description=filter_description,
            participant_count_before=self.current_count,
            participant_count_after=new_count,
            participants_removed=participants_removed,
            removal_percentage=removal_percentage,
            demographics_before=demographics_before or {},
            demographics_after=demographics_after or {}
        )
        
        self.steps.append(step)
        self.current_count = new_count
        
        return step
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all filter steps."""
        total_removed = self.initial_count - self.current_count
        total_removal_pct = (total_removed / self.initial_count * 100) if self.initial_count > 0 else 0
        
        return {
            'initial_participants': self.initial_count,
            'final_participants': self.current_count,
            'total_removed': total_removed,
            'total_removal_percentage': total_removal_pct,
            'number_of_steps': len(self.steps),
            'steps': [{
                'step': step.step_number,
                'type': step.filter_type,
                'description': step.filter_description,
                'removed': step.participants_removed,
                'remaining': step.participant_count_after,
                'removal_pct': step.removal_percentage
            } for step in self.steps]
        }


def generate_filtering_report(
    config_params: Dict[str, Any],
    merge_keys: MergeKeys,
    demographic_filters: Dict[str, Any],
    behavioral_filters: List[Dict[str, Any]],
    tables_to_join: List[str]
) -> pd.DataFrame:
    """
    Create comprehensive filtering steps report.
    
    Args:
        config_params: Configuration parameters
        merge_keys: Merge strategy information
        demographic_filters: Demographic filter parameters
        behavioral_filters: List of behavioral filter definitions
        tables_to_join: List of tables to join
        
    Returns:
        DataFrame with filtering progression analysis
        
    Raises:
        DataProcessingError: If report generation fails
    """
    try:
        from query.query_secure import generate_base_query_logic_secure
        
        db_manager = get_database_manager()
        tracker = FilterTracker()
        
        # Step 0: Get initial participant count (no filters)
        base_query_no_filters, base_params = generate_base_query_logic_secure(
            config_params, merge_keys, {}, [], tables_to_join
        )
        
        # Count participants with no filters
        count_column = merge_keys.composite_id if merge_keys.is_longitudinal and merge_keys.composite_id else merge_keys.primary_id
        initial_count_query = f"SELECT COUNT(DISTINCT demo.{count_column}) as count {base_query_no_filters}"
        
        initial_result = db_manager.execute_query_single(initial_count_query, base_params)
        initial_count = initial_result[0] if initial_result else 0
        
        tracker.initial_count = initial_count
        tracker.current_count = initial_count
        
        # Get initial demographics
        initial_demographics = calculate_demographics_breakdown(
            config_params, merge_keys, base_query_no_filters, base_params
        )
        
        # Apply filters step by step and track impact
        current_demographic_filters = {}
        current_behavioral_filters = []
        
        # Step 1: Apply age filter
        age_range = demographic_filters.get('age_range')
        if age_range:
            current_demographic_filters['age_range'] = age_range
            
            query, params = generate_base_query_logic_secure(
                config_params, merge_keys, current_demographic_filters, current_behavioral_filters, tables_to_join
            )
            
            count_query = f"SELECT COUNT(DISTINCT demo.{count_column}) as count {query}"
            result = db_manager.execute_query_single(count_query, params)
            new_count = result[0] if result else 0
            
            demographics_after = calculate_demographics_breakdown(
                config_params, merge_keys, query, params
            )
            
            tracker.add_step(
                'demographic',
                f"Age filter: {age_range[0]}-{age_range[1]} years",
                new_count,
                initial_demographics,
                demographics_after
            )
        
        # Step 2: Apply session filter (for longitudinal data)
        sessions = demographic_filters.get('sessions')
        if sessions and merge_keys.is_longitudinal:
            current_demographic_filters['sessions'] = sessions
            
            query, params = generate_base_query_logic_secure(
                config_params, merge_keys, current_demographic_filters, current_behavioral_filters, tables_to_join
            )
            
            count_query = f"SELECT COUNT(DISTINCT demo.{count_column}) as count {query}"
            result = db_manager.execute_query_single(count_query, params)
            new_count = result[0] if result else 0
            
            demographics_after = calculate_demographics_breakdown(
                config_params, merge_keys, query, params
            )
            
            session_desc = f"Session filter: {', '.join(map(str, sessions))}"
            tracker.add_step(
                'demographic',
                session_desc,
                new_count,
                tracker.steps[-1].demographics_after if tracker.steps else initial_demographics,
                demographics_after
            )
        
        # Step 3: Apply substudy filter
        substudies = demographic_filters.get('substudies')
        if substudies:
            current_demographic_filters['substudies'] = substudies
            
            query, params = generate_base_query_logic_secure(
                config_params, merge_keys, current_demographic_filters, current_behavioral_filters, tables_to_join
            )
            
            count_query = f"SELECT COUNT(DISTINCT demo.{count_column}) as count {query}"
            result = db_manager.execute_query_single(count_query, params)
            new_count = result[0] if result else 0
            
            demographics_after = calculate_demographics_breakdown(
                config_params, merge_keys, query, params
            )
            
            substudy_desc = f"Substudy filter: {', '.join(substudies)}"
            tracker.add_step(
                'demographic',
                substudy_desc,
                new_count,
                tracker.steps[-1].demographics_after if tracker.steps else initial_demographics,
                demographics_after
            )
        
        # Step 4+: Apply behavioral filters one by one
        for i, filter_def in enumerate(behavioral_filters):
            current_behavioral_filters.append(filter_def)
            
            query, params = generate_base_query_logic_secure(
                config_params, merge_keys, current_demographic_filters, current_behavioral_filters, tables_to_join
            )
            
            count_query = f"SELECT COUNT(DISTINCT demo.{count_column}) as count {query}"
            result = db_manager.execute_query_single(count_query, params)
            new_count = result[0] if result else 0
            
            demographics_after = calculate_demographics_breakdown(
                config_params, merge_keys, query, params
            )
            
            # Create filter description
            table_name = filter_def.get('table', 'unknown')
            column_name = filter_def.get('column', 'unknown')
            filter_type = filter_def.get('type', 'unknown')
            value = filter_def.get('value', 'unknown')
            
            if filter_type == 'range':
                filter_desc = f"{table_name}.{column_name}: {value[0]}-{value[1]}"
            elif filter_type == 'categorical':
                if isinstance(value, list) and len(value) <= 3:
                    filter_desc = f"{table_name}.{column_name}: {', '.join(map(str, value))}"
                else:
                    filter_desc = f"{table_name}.{column_name}: {len(value)} values"
            else:
                filter_desc = f"{table_name}.{column_name}: {filter_type}"
            
            tracker.add_step(
                'phenotypic',
                filter_desc,
                new_count,
                tracker.steps[-1].demographics_after if tracker.steps else initial_demographics,
                demographics_after
            )
        
        # Convert to DataFrame
        report_data = []
        
        # Initial state
        report_data.append({
            'Step': 0,
            'Filter Type': 'Initial',
            'Filter Description': 'No filters applied',
            'Participants Before': '-',
            'Participants After': initial_count,
            'Participants Removed': 0,
            'Removal %': 0.0,
            'Cumulative Removal %': 0.0
        })
        
        # Filter steps
        cumulative_removed = 0
        for step in tracker.steps:
            cumulative_removed += step.participants_removed
            cumulative_removal_pct = (cumulative_removed / initial_count * 100) if initial_count > 0 else 0
            
            report_data.append({
                'Step': step.step_number,
                'Filter Type': step.filter_type.title(),
                'Filter Description': step.filter_description,
                'Participants Before': step.participant_count_before,
                'Participants After': step.participant_count_after,
                'Participants Removed': step.participants_removed,
                'Removal %': round(step.removal_percentage, 2),
                'Cumulative Removal %': round(cumulative_removal_pct, 2)
            })
        
        report_df = pd.DataFrame(report_data)
        
        return report_df
    
    except Exception as e:
        error_msg = f"Error generating filtering report: {e}"
        logging.error(error_msg)
        raise DataProcessingError(error_msg, details={
            'demographic_filters': demographic_filters,
            'behavioral_filters_count': len(behavioral_filters)
        })


def validate_behavioral_filters(behavioral_filters: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
    """
    Validate behavioral filter definitions.
    
    Args:
        behavioral_filters: List of behavioral filter definitions
        
    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []
    
    try:
        if not isinstance(behavioral_filters, list):
            errors.append("Behavioral filters must be a list")
            return False, errors
        
        for i, filter_def in enumerate(behavioral_filters):
            if not isinstance(filter_def, dict):
                errors.append(f"Behavioral filter {i} must be a dictionary")
                continue
            
            # Check required fields
            required_fields = ['table', 'column', 'type', 'value']
            for field in required_fields:
                if field not in filter_def:
                    errors.append(f"Behavioral filter {i} missing required field: {field}")
            
            # Validate filter type
            filter_type = filter_def.get('type')
            if filter_type not in ['range', 'categorical']:
                errors.append(f"Behavioral filter {i} has invalid type: {filter_type}")
            
            # Validate value based on type
            value = filter_def.get('value')
            if filter_type == 'range':
                if not isinstance(value, (list, tuple)) or len(value) != 2:
                    errors.append(f"Behavioral filter {i} range value must be a list/tuple of 2 values")
                else:
                    try:
                        float(value[0])
                        float(value[1])
                        if float(value[0]) >= float(value[1]):
                            errors.append(f"Behavioral filter {i} range minimum must be less than maximum")
                    except (ValueError, TypeError):
                        errors.append(f"Behavioral filter {i} range values must be numeric")
            
            elif filter_type == 'categorical':
                if not isinstance(value, (list, tuple)):
                    errors.append(f"Behavioral filter {i} categorical value must be a list/tuple")
                elif len(value) == 0:
                    errors.append(f"Behavioral filter {i} categorical value cannot be empty")
        
        return len(errors) == 0, errors
    
    except Exception as e:
        errors.append(f"Error validating behavioral filters: {e}")
        return False, errors


def analyze_filter_impact(
    config_params: Dict[str, Any],
    merge_keys: MergeKeys,
    demographic_filters: Dict[str, Any],
    behavioral_filters: List[Dict[str, Any]],
    tables_to_join: List[str]
) -> Dict[str, Any]:
    """
    Analyze the impact of different filters on sample size.
    
    Args:
        config_params: Configuration parameters
        merge_keys: Merge strategy information
        demographic_filters: Demographic filter parameters
        behavioral_filters: List of behavioral filter definitions
        tables_to_join: List of tables to join
        
    Returns:
        Dictionary with filter impact analysis
        
    Raises:
        DataProcessingError: If analysis fails
    """
    try:
        from query.query_secure import generate_base_query_logic_secure
        
        db_manager = get_database_manager()
        
        # Get baseline count (no filters)
        base_query, base_params = generate_base_query_logic_secure(
            config_params, merge_keys, {}, [], tables_to_join
        )
        
        count_column = merge_keys.composite_id if merge_keys.is_longitudinal and merge_keys.composite_id else merge_keys.primary_id
        base_count_query = f"SELECT COUNT(DISTINCT demo.{count_column}) as count {base_query}"
        
        base_result = db_manager.execute_query_single(base_count_query, base_params)
        baseline_count = base_result[0] if base_result else 0
        
        impact_analysis = {
            'baseline_count': baseline_count,
            'demographic_impact': {},
            'behavioral_impact': {},
            'combined_impact': {}
        }
        
        # Analyze demographic filter impact individually
        for filter_name, filter_value in demographic_filters.items():
            if filter_value:
                single_filter = {filter_name: filter_value}
                
                query, params = generate_base_query_logic_secure(
                    config_params, merge_keys, single_filter, [], tables_to_join
                )
                
                count_query = f"SELECT COUNT(DISTINCT demo.{count_column}) as count {query}"
                result = db_manager.execute_query_single(count_query, params)
                filtered_count = result[0] if result else 0
                
                removed = baseline_count - filtered_count
                removal_pct = (removed / baseline_count * 100) if baseline_count > 0 else 0
                
                impact_analysis['demographic_impact'][filter_name] = {
                    'remaining_count': filtered_count,
                    'removed_count': removed,
                    'removal_percentage': removal_pct
                }
        
        # Analyze behavioral filter impact individually
        for i, filter_def in enumerate(behavioral_filters):
            query, params = generate_base_query_logic_secure(
                config_params, merge_keys, {}, [filter_def], tables_to_join
            )
            
            count_query = f"SELECT COUNT(DISTINCT demo.{count_column}) as count {query}"
            result = db_manager.execute_query_single(count_query, params)
            filtered_count = result[0] if result else 0
            
            removed = baseline_count - filtered_count
            removal_pct = (removed / baseline_count * 100) if baseline_count > 0 else 0
            
            filter_name = f"{filter_def.get('table', 'unknown')}.{filter_def.get('column', 'unknown')}"
            
            impact_analysis['behavioral_impact'][filter_name] = {
                'remaining_count': filtered_count,
                'removed_count': removed,
                'removal_percentage': removal_pct
            }
        
        # Analyze combined impact
        combined_query, combined_params = generate_base_query_logic_secure(
            config_params, merge_keys, demographic_filters, behavioral_filters, tables_to_join
        )
        
        combined_count_query = f"SELECT COUNT(DISTINCT demo.{count_column}) as count {combined_query}"
        combined_result = db_manager.execute_query_single(combined_count_query, combined_params)
        final_count = combined_result[0] if combined_result else 0
        
        total_removed = baseline_count - final_count
        total_removal_pct = (total_removed / baseline_count * 100) if baseline_count > 0 else 0
        
        impact_analysis['combined_impact'] = {
            'final_count': final_count,
            'total_removed': total_removed,
            'total_removal_percentage': total_removal_pct,
            'filter_efficiency': {
                'demographic_filters': len([f for f in demographic_filters.values() if f]),
                'behavioral_filters': len(behavioral_filters),
                'total_filters': len([f for f in demographic_filters.values() if f]) + len(behavioral_filters)
            }
        }
        
        return impact_analysis
    
    except Exception as e:
        error_msg = f"Error analyzing filter impact: {e}"
        logging.error(error_msg)
        raise DataProcessingError(error_msg, details={
            'demographic_filters': demographic_filters,
            'behavioral_filters_count': len(behavioral_filters)
        })