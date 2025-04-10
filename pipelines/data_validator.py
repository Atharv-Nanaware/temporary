"""
Data validator module for ETL pipeline.
Contains validation rules to ensure data integrity before loading to Kafka.
"""
import logging
import re
from datetime import datetime
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("data_validator")

# Validation configuration
REQUIRED_FIELDS = ["username", "email", "first_name", "last_name", "registered_date"]
EMAIL_PATTERN = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
DATE_PATTERN = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z$'

# Data type validation rules
FIELD_TYPES = {
    "first_name": str,
    "last_name": str,
    "gender": str,
    "address": str,
    "post_code": [str, int],  # Can be string or integer
    "email": str,
    "username": str,
    "dob": str,
    "registered_date": str,
    "phone": str,
    "picture": str
}

# Value constraints
VALUE_CONSTRAINTS = {
    "gender": ["male", "female"],  # Allowed values for gender
    "username": {"min_length": 3, "max_length": 30}  # Length constraints for username
}

def validate_required_fields(record):
    """
    Check if all required fields are present and non-empty.
    
    Args:
        record (dict): User data record
        
    Returns:
        bool: True if all required fields are present and non-empty
    """
    for field in REQUIRED_FIELDS:
        if field not in record or not record[field]:
            logger.warning(f"Missing required field: {field}")
            return False
    return True

def validate_email_format(email):
    """
    Validate email format using regex pattern.
    
    Args:
        email (str): Email address to validate
        
    Returns:
        bool: True if email format is valid
    """
    if not email:
        return False
    return bool(re.match(EMAIL_PATTERN, email))

def validate_date_format(date_str):
    """
    Validate date format.
    
    Args:
        date_str (str): Date string to validate
        
    Returns:
        bool: True if date format is valid
    """
    if not date_str:
        return False
    return bool(re.match(DATE_PATTERN, date_str))

def validate_data_types(record):
    """
    Validate data types for each field.
    
    Args:
        record (dict): User data record
        
    Returns:
        tuple: (bool, list) - (is_valid, invalid_fields)
    """
    invalid_fields = []
    
    for field, expected_type in FIELD_TYPES.items():
        if field in record:
            # Handle multiple allowed types
            if isinstance(expected_type, list):
                if not any(isinstance(record[field], t) for t in expected_type):
                    invalid_fields.append(field)
            # Handle single type
            elif not isinstance(record[field], expected_type):
                invalid_fields.append(field)
    
    return len(invalid_fields) == 0, invalid_fields

def validate_value_constraints(record):
    """
    Validate value constraints for specific fields.
    
    Args:
        record (dict): User data record
        
    Returns:
        tuple: (bool, dict) - (is_valid, constraint_violations)
    """
    constraint_violations = {}
    
    # Check enum constraints (values from a predefined list)
    for field, allowed_values in {k: v for k, v in VALUE_CONSTRAINTS.items() 
                                if isinstance(v, list)}.items():
        if field in record and record[field] not in allowed_values:
            constraint_violations[field] = f"Value must be one of: {', '.join(allowed_values)}"
    
    # Check length constraints
    for field, constraints in {k: v for k, v in VALUE_CONSTRAINTS.items() 
                            if isinstance(v, dict)}.items():
        if field in record:
            value = record[field]
            if "min_length" in constraints and len(value) < constraints["min_length"]:
                constraint_violations[field] = f"Minimum length: {constraints['min_length']}"
            if "max_length" in constraints and len(value) > constraints["max_length"]:
                constraint_violations[field] = f"Maximum length: {constraints['max_length']}"
    
    return len(constraint_violations) == 0, constraint_violations

def validate_record(record, usernames_seen=None):
    """
    Validate a single record against all validation rules.
    
    Args:
        record (dict): User data record
        usernames_seen (set, optional): Set of usernames already seen
        
    Returns:
        tuple: (bool, dict) - (is_valid, validation_details)
    """
    validation_details = {
        "required_fields": validate_required_fields(record)
    }
    
    # Only check email format if the field exists
    if "email" in record:
        validation_details["email_format"] = validate_email_format(record["email"])
    else:
        validation_details["email_format"] = False
        
    # Only check date format if the field exists
    if "registered_date" in record:
        validation_details["date_format"] = validate_date_format(record["registered_date"])
    else:
        validation_details["date_format"] = False
    
    # Check username uniqueness if set is provided
    if usernames_seen is not None and "username" in record and record["username"]:
        validation_details["username_unique"] = record["username"] not in usernames_seen
    else:
        validation_details["username_unique"] = True
    
    # Check data types
    is_valid_types, invalid_fields = validate_data_types(record)
    validation_details["valid_data_types"] = is_valid_types
    if not is_valid_types:
        validation_details["invalid_data_types"] = invalid_fields
    
    # Check value constraints
    is_valid_constraints, constraint_violations = validate_value_constraints(record)
    validation_details["valid_constraints"] = is_valid_constraints
    if not is_valid_constraints:
        validation_details["constraint_violations"] = constraint_violations
    
    # Record is valid if all checks pass
    is_valid = all([
        validation_details["required_fields"],
        validation_details["email_format"],
        validation_details["date_format"],
        validation_details["username_unique"],
        validation_details["valid_data_types"],
        validation_details["valid_constraints"]
    ])
    
    return is_valid, validation_details

def validate_batch(records):
    """
    Validate a batch of records.
    
    Args:
        records (list): List of user data records
        
    Returns:
        tuple: (valid_records, invalid_records, validation_summary)
    """
    valid_records = []
    invalid_records = []
    validation_summary = {
        "total_records": len(records),
        "valid_records": 0,
        "invalid_records": 0,
        "validation_failures": {
            "required_fields": 0,
            "email_format": 0,
            "date_format": 0,
            "username_unique": 0,
            "data_types": 0,
            "value_constraints": 0
        }
    }
    
    # Track username uniqueness
    usernames_seen = set()
    
    for record in records:
        # Check if username already exists in the batch
        if "username" in record and record["username"]:
            username_unique = record["username"] not in usernames_seen
            if username_unique:
                usernames_seen.add(record["username"])
        
        # Validate the record
        is_valid, validation_details = validate_record(record, usernames_seen)
        
        # Update validation summary
        for rule, passed in validation_details.items():
            if rule in validation_summary["validation_failures"] and not passed:
                validation_summary["validation_failures"][rule] += 1
        
        # Sort record into valid or invalid
        if is_valid:
            valid_records.append(record)
            validation_summary["valid_records"] += 1
        else:
            # Add validation details to invalid record for debugging
            invalid_record = record.copy()
            invalid_record["_validation_details"] = validation_details
            invalid_records.append(invalid_record)
            validation_summary["invalid_records"] += 1
    
    # Log validation summary
    logger.info(f"Data validation completed: {validation_summary['valid_records']} valid, "
               f"{validation_summary['invalid_records']} invalid records")
    
    if validation_summary["invalid_records"] > 0:
        logger.warning("Validation failures breakdown: "
                      f"{json.dumps(validation_summary['validation_failures'])}")
    
    return valid_records, invalid_records, validation_summary

def get_validation_report(invalid_records):
    """
    Generate a detailed validation report for invalid records.
    
    Args:
        invalid_records (list): List of invalid records with validation details
        
    Returns:
        dict: Detailed validation report
    """
    report = {
        "total_invalid": len(invalid_records),
        "issues_by_field": {},
        "issues_by_rule": {
            "required_fields": 0,
            "email_format": 0,
            "date_format": 0,
            "username_unique": 0,
            "data_types": 0,
            "value_constraints": 0
        }
    }
    
    for record in invalid_records:
        if "_validation_details" in record:
            details = record["_validation_details"]
            
            # Count issues by rule
            for rule, passed in details.items():
                if rule in report["issues_by_rule"] and not passed:
                    report["issues_by_rule"][rule] += 1
            
            # Count issues by field
            if "invalid_data_types" in details:
                for field in details["invalid_data_types"]:
                    if field not in report["issues_by_field"]:
                        report["issues_by_field"][field] = {"count": 0, "issues": []}
                    report["issues_by_field"][field]["count"] += 1
                    report["issues_by_field"][field]["issues"].append("Invalid data type")
            
            if "constraint_violations" in details:
                for field, message in details["constraint_violations"].items():
                    if field not in report["issues_by_field"]:
                        report["issues_by_field"][field] = {"count": 0, "issues": []}
                    report["issues_by_field"][field]["count"] += 1
                    report["issues_by_field"][field]["issues"].append(f"Constraint violation: {message}")
    
    return report
