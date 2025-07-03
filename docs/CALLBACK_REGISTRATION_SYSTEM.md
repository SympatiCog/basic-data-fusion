# Enhanced Callback Registration System

## Overview

The enhanced callback registration system provides a robust, monitored, and fault-tolerant way to register all query page callbacks with the Dash application. This system was implemented as part of Phase 5.2 of the query module refactoring.

## Architecture

### Components

1. **Main Registration Function** (`register_all_callbacks`)
   - Orchestrates registration of all callback modules
   - Provides detailed performance monitoring
   - Handles errors gracefully with partial failure recovery
   - Prevents duplicate registrations

2. **Module Registry** (`CALLBACK_MODULES`)
   - Defines expected callback modules and their characteristics
   - Specifies minimum callback counts for validation
   - Provides descriptive information for each module

3. **Statistics Tracking**
   - Tracks registration performance metrics
   - Records per-module callback counts and timing
   - Maintains error logs and success status

4. **Utility Functions**
   - `get_registration_stats()` - Retrieve registration statistics
   - `is_registered()` - Check registration status
   - `unregister_callbacks()` - Mark callbacks as unregistered

## Usage

### Basic Registration

```python
from query.callbacks import register_all_callbacks
import dash

app = dash.Dash(__name__)
stats = register_all_callbacks(app)

print(f"Registered {stats['total_callbacks']} callbacks in {stats['duration_ms']}ms")
```

### Detailed Monitoring

```python
# Register with verbose output
stats = register_all_callbacks(app, verbose=True)

# Check registration status
from query.callbacks import is_registered
if is_registered(app):
    print("Callbacks are registered")

# Get detailed statistics
from query.callbacks import get_registration_stats
stats = get_registration_stats(id(app))
for module_name, module_stats in stats['modules'].items():
    print(f"{module_name}: {module_stats['callbacks_registered']} callbacks")
```

### Error Handling

```python
try:
    stats = register_all_callbacks(app)
    if not stats['success']:
        print(f"Registration issues: {stats['errors']}")
except RuntimeError as e:
    print(f"Complete registration failure: {e}")
```

## Callback Modules

The system manages four main callback modules:

### 1. Data Loading (`data_loading`)
- **Purpose**: Data status and table information callbacks
- **Min Callbacks**: 3
- **Functions**: Load initial data, update status sections, manage table options

### 2. Filters (`filters`) 
- **Purpose**: Demographic and phenotypic filter management
- **Min Callbacks**: 4
- **Functions**: Age sliders, dynamic filters, participant count updates

### 3. Export (`export`)
- **Purpose**: Data export and generation
- **Min Callbacks**: 2
- **Functions**: Data generation, download handling, export modals

### 4. State (`state`)
- **Purpose**: State management and persistence
- **Min Callbacks**: 15
- **Functions**: Store updates, value restoration, session persistence

## Registration Statistics

The system provides comprehensive statistics for monitoring and debugging:

```python
{
    'app_id': 140123456789,
    'start_time': 1625123456.789,
    'end_time': 1625123456.790,
    'duration_ms': 15.23,
    'total_callbacks': 36,
    'success': True,
    'errors': [],
    'modules': {
        'data_loading': {
            'success': True,
            'callbacks_registered': 5,
            'duration_ms': 2.1,
            'description': 'Data loading and status'
        },
        'filters': {
            'success': True,
            'callbacks_registered': 6,
            'duration_ms': 3.2,
            'description': 'Filter management'
        },
        # ... more modules
    }
}
```

## Error Handling

### Duplicate Registration Prevention

The system prevents duplicate registrations for the same app instance:

```python
# First registration
stats1 = register_all_callbacks(app)  # Registers callbacks

# Second registration  
stats2 = register_all_callbacks(app)  # Returns cached stats, no duplicate registration
```

### Partial Failure Recovery

If some modules fail to register, the system continues with successful modules:

```python
# If data_loading fails but others succeed:
stats = register_all_callbacks(app)
# stats['success'] may be True even with some failures
# Check stats['errors'] for details
```

### Complete Failure Handling

If all modules fail, a RuntimeError is raised:

```python
try:
    register_all_callbacks(app)
except RuntimeError as e:
    # All modules failed to register
    print(f"Complete failure: {e}")
```

## Performance Monitoring

### Timing Information

Each registration provides detailed timing:

- **Total Duration**: Overall registration time
- **Per-Module Duration**: Time taken by each module
- **Start/End Timestamps**: Absolute timing information

### Callback Counting

The system attempts to count registered callbacks:

- **Accurate Counting**: When Dash callback map is accessible
- **Estimated Counting**: When direct counting isn't possible
- **Validation**: Warns if callback count is below expected minimum

## Best Practices

### 1. Register Early

Register callbacks as early as possible in app initialization:

```python
app = dash.Dash(__name__)
register_all_callbacks(app)  # Register immediately after app creation
```

### 2. Handle Errors Gracefully

Always check registration success:

```python
stats = register_all_callbacks(app)
if not stats['success']:
    logging.warning(f"Registration issues: {stats['errors']}")
```

### 3. Monitor Performance

Use statistics to monitor registration performance:

```python
stats = register_all_callbacks(app)
if stats['duration_ms'] > 100:  # Alert if registration takes too long
    logging.warning(f"Slow registration: {stats['duration_ms']}ms")
```

### 4. Use Verbose Mode for Debugging

Enable verbose output during development:

```python
# Development
stats = register_all_callbacks(app, verbose=True)

# Production
stats = register_all_callbacks(app, verbose=False)
```

## Testing

Comprehensive tests are available in `tests/test_callback_registration.py`:

```bash
pytest tests/test_callback_registration.py -v
```

### Test Coverage

- ✅ Successful registration
- ✅ Duplicate prevention
- ✅ Error handling (partial and complete failures)
- ✅ Statistics tracking
- ✅ Performance monitoring
- ✅ Module validation
- ✅ Integration testing

## Migration from Simple Registration

### Before (Simple)

```python
from query.callbacks import register_all_callbacks

app = dash.Dash(__name__)
register_all_callbacks(app)
print("Callbacks registered")
```

### After (Enhanced)

```python
from query.callbacks import register_all_callbacks

app = dash.Dash(__name__)
stats = register_all_callbacks(app, verbose=True)

if stats['success']:
    print(f"✓ {stats['total_callbacks']} callbacks registered in {stats['duration_ms']}ms")
else:
    print(f"✗ Registration issues: {stats['errors']}")
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Check that all callback modules are available
2. **Missing Functions**: Ensure each module has `register_callbacks(app)` function
3. **Performance Issues**: Check individual module timing in statistics
4. **Callback Count Warnings**: Verify modules are registering expected callbacks

### Debug Information

```python
from query.callbacks import get_registration_stats, CALLBACK_MODULES

# Check module configuration
print("Expected modules:", CALLBACK_MODULES)

# Get registration details
stats = get_registration_stats()
for app_id, app_stats in stats.items():
    print(f"App {app_id}: {app_stats['total_callbacks']} callbacks")
```

## Future Enhancements

Potential improvements for future versions:

1. **Callback Dependency Tracking**: Ensure callbacks are registered in correct order
2. **Health Checks**: Periodic validation of callback functionality
3. **Hot Reloading**: Support for dynamic callback re-registration
4. **Metrics Export**: Integration with monitoring systems
5. **Configuration Validation**: Verify callback configurations match expectations