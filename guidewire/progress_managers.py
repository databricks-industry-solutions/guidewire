"""
Redesigned progress management system with separate implementations.
Includes SimpleProgressManager for sequential processing and 
MultiProgressManager for Ray-based parallel processing.
"""
import ray
import time
import threading
from typing import List, Dict, Any, Optional, Tuple
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.live import Live
from guidewire.logging import logger as L


@ray.remote
class MultiProgressTracker:
    """Ray actor to track progress across multiple workers/tables."""
    
    def __init__(self):
        """Initialize the progress tracker."""
        self.progress = {}  # {worker_id: (count, total, msg, status, start_time)}
        self.completed_workers = set()
        
    def register_table(self, table_id: str, schema_timestamp: int = 0):
        """Register a new table for tracking with placeholder total.
        
        Args:
            table_id: Unique identifier for the table/worker
            schema_timestamp: Schema timestamp for this table
        """
        placeholder_total = 1  # Small placeholder total that will be updated when actual total is known
        self.progress[table_id] = (0, placeholder_total, "Registered", "registered", time.time())
        L.debug(f"Registered table {table_id} with placeholder total")
        
    def start_table(self, table_id: str, total_folders: int):
        """Mark a table as started with the actual total.
        
        Args:
            table_id: Unique identifier for the table/worker
            total_folders: Total number of folders to process (mandatory)
        """
        if table_id in self.progress:
            count, _, _, _, _ = self.progress[table_id]
            self.progress[table_id] = (count, total_folders, "", "running", time.time())
            L.debug(f"Started processing table {table_id} with {total_folders} folders")
        else:
            # Auto-register if not already registered
            self.progress[table_id] = (0, total_folders, "", "running", time.time())
            L.debug(f"Auto-registered and started table {table_id} with {total_folders} folders")
        
    def update_progress(self, table_id: str, count: int, total: int, msg: str = ""):
        """Update progress for a specific table/worker.
        
        Args:
            table_id: Unique identifier for the table/worker
            count: Current progress count
            total: Total items to process
            msg: Optional status message
        """
        if table_id in self.progress:
            _, _, old_msg, status, start_time = self.progress[table_id]
            new_msg = msg or old_msg
            self.progress[table_id] = (count, total, new_msg, status, start_time)
        
    def complete_table(self, table_id: str, error_message: Optional[str] = None):
        """Mark a table as completed.
        
        Args:
            table_id: Unique identifier for the table/worker
            error_message: Optional error message if completion was due to error
        """
        if table_id in self.progress:
            count, total, _, _, start_time = self.progress[table_id]
            status = "error" if error_message else "completed"
            msg = error_message or "Completed"
            self.progress[table_id] = (total, total, msg, status, start_time)
            self.completed_workers.add(table_id)
            L.debug(f"Completed table {table_id} with status: {status}")
    
    def get_all_progress(self) -> Dict[str, Tuple[int, int, str, str, float]]:
        """Get progress for all tracked tables.
        
        Returns:
            Dictionary mapping table_id to (count, total, msg, status, start_time)
        """
        return self.progress.copy()
    
    def get_progress(self, table_id: str) -> Optional[Tuple[int, int, str, str, float]]:
        """Get progress for a specific table.
        
        Args:
            table_id: Unique identifier for the table/worker
            
        Returns:
            Tuple of (count, total, msg, status, start_time) or None if not found
        """
        return self.progress.get(table_id)
    
    def is_complete(self, table_id: str) -> bool:
        """Check if a table is complete.
        
        Args:
            table_id: Unique identifier for the table/worker
            
        Returns:
            True if the table is marked as complete
        """
        return table_id in self.completed_workers
    
    def get_completed_count(self) -> int:
        """Get the number of completed tables."""
        return len(self.completed_workers)
    
    def get_total_count(self) -> int:
        """Get the total number of registered tables."""
        return len(self.progress)
    
    def is_all_complete(self) -> bool:
        """Check if all registered tables are complete."""
        return len(self.completed_workers) >= len(self.progress)
    
    def get_summary(self) -> Dict:
        """Get a summary of all progress.
        
        Returns:
            Dictionary with summary statistics
        """
        total_tables = len(self.progress)
        completed_tables = len(self.completed_workers)
        
        total_items = sum(total for _, total, _, _, _ in self.progress.values())
        completed_items = sum(count for count, _, _, status, _ in self.progress.values() 
                            if status in ["completed", "error"])
        
        return {
            "total_tables": total_tables,
            "completed_tables": completed_tables,
            "total_items": total_items,
            "completed_items": completed_items,
            "completion_percentage": (completed_items / total_items * 100) if total_items > 0 else 0
        }


def _create_progress_table(tables_data: Dict[str, Dict[str, Any]], current_time: float) -> Table:
    """Create a Rich progress table from tables data.
    
    Args:
        tables_data: Dictionary mapping table_id to progress information
        current_time: Current timestamp for rate calculations
        
    Returns:
        Rich Table instance
    """
    # Calculate table width based on column widths + padding for borders and spacing
    # Reduced widths to fit in Jupyter/IPython widget display
    column_widths = [22, 3, 12, 10, 5, 5, 15]  # Table ID, Status, Progress, Folders, Rate, ETA, Message
    table_width = sum(column_widths) + len(column_widths) * 2 + 4  # 2 chars per column for padding/borders, 4 for outer borders
    
    # Create Rich
    table = Table(title=f"🚀 Progress Update {time.strftime('%H:%M:%S')}", 
                 title_style="bold red",
                 show_header=True, 
                 header_style="bold red",
                 border_style="red",
                 width=table_width)
    
    # Add columns with better width control
    table.add_column("Table ID", style="red",  width=22)
    table.add_column("St", justify="center", style="bold", width=3)
    table.add_column("Progress", justify="center", width=12)
    table.add_column("Folders", justify="right", style="green", width=10)
    table.add_column("Rate", justify="right", style="yellow", width=5)
    table.add_column("ETA", justify="right", style="blue", width=5)
    table.add_column("Message", style="dim", max_width=15)
    
    # Filter out successfully completed tables, keep only running, pending, registered, and error tables
    filtered_tables = {
        table_id: table_data for table_id, table_data in tables_data.items()
        if table_data.get("status") != "completed" or table_data.get("error") is not None
    }
    
    # Sort tables by status (running first, then registered, then pending, then errors)
    status_order = {"running": 0, "registered": 1, "pending": 2, "error": 3}
    sorted_tables = sorted(
        filtered_tables.items(), 
        key=lambda x: (status_order.get(x[1].get("status", "pending"), 4), x[0])
    )
    
    # Count all tables for accurate summary (including filtered out completed ones)
    total_tables = len(tables_data)
    completed_tables = sum(1 for table_data in tables_data.values() 
                          if table_data.get("status") == "completed" and table_data.get("error") is None)
    error_tables = sum(1 for table_data in tables_data.values() 
                      if table_data.get("error") is not None)

    
    # Process only the filtered (displayed) tables for the table rows
    for table_id, table_data in sorted_tables:
        count = table_data.get("count", 0)
        total = table_data.get("total", 0)
        status = table_data.get("status", "pending")
        message = table_data.get("message", "")
        start_time = table_data.get("start_time")
        error = table_data.get("error")
        
        # Note: Counters are calculated upfront now, no need to update in loop
        
        # Calculate progress percentage
        percentage = (count / total * 100) if total > 0 else 0
        
        # Calculate processing rate and ETA
        rate_str = "N/A"
        eta_str = "N/A"
        
        if start_time and status == "running" and count > 0:
            elapsed = current_time - start_time
            if elapsed > 0:
                rate = count / elapsed
                # Format rate based on scale for better readability
                if rate >= 1:
                    rate_str = f"{rate:.1f}/s"
                elif rate >= 0.1:
                    rate_str = f"{rate:.2f}/s"
                elif rate >= 0.01:
                    # Show as folders per minute for very slow rates
                    rate_per_minute = rate * 60
                    rate_str = f"{rate_per_minute:.1f}/m"
                else:
                    # Show as folders per hour for extremely slow rates
                    rate_per_hour = rate * 3600
                    rate_str = f"{rate_per_hour:.1f}/h"
                
                if rate > 0 and count < total:
                    remaining = total - count
                    eta_seconds = remaining / rate
                    if eta_seconds < 60:
                        eta_str = f"{eta_seconds:.0f}s"
                    elif eta_seconds < 3600:
                        eta_str = f"{eta_seconds/60:.0f}m"
                    else:
                        eta_str = f"{eta_seconds/3600:.1f}h"
        
        # Create Rich progress bar to fill the column better
        if total > 0:
            bar_length = 10  # Longer bar to fill 12-char column better
            filled = int(bar_length * count / total)
            progress_bar = f"[{'█' * filled}{'░' * (bar_length - filled)}]"
        else:
            progress_bar = f"[{'░' * 10}]"
        
        # Status with symbols and colors
        status_styles = {
            "pending": ("⏳", "yellow"),
            "registered": ("📋", "blue"), 
            "running": ("🚀", "green"),
            "completed": ("✅", "bright_green"),
            "error": ("❌", "red")
        }
        
        # Use error status if there's an error
        if error:
            status = "error"
            message = error
        
        status_text, status_style = status_styles.get(status, ("❓ Unknown", "white"))
        status_display = Text(status_text, style=status_style)
        
        # Format as "completed/total" with color coding based on percentage
        folder_display = f"{count}/{total}"
        if percentage == 100:
            percentage_display = Text(folder_display, style="bright_green bold")
        elif percentage >= 75:
            percentage_display = Text(folder_display, style="green")
        elif percentage >= 50:
            percentage_display = Text(folder_display, style="yellow")
        elif percentage >= 25:
            percentage_display = Text(folder_display, style="orange3")
        else:
            percentage_display = Text(folder_display, style="red")
        
        # Truncate message if too long, or show empty for normal processing
        display_message = message.strip() if message else ""
        if len(display_message) > 15:
            display_message = display_message[:12] + "..."
        
        # Truncate table ID if too long
        display_table_id = table_id
        if len(table_id) > 22:
            display_table_id = table_id[:19] + "..."
        
        # Add row to table
        table.add_row(
            display_table_id,
            status_display,
            progress_bar,
            percentage_display,
            rate_str,
            eta_str,
            display_message
        )
    
    # Add summary as a footer or separate section
    overall_percentage = (completed_tables / total_tables * 100) if total_tables > 0 else 0
    summary_text = f"📊 Summary: {completed_tables}/{total_tables} tables complete ({overall_percentage:.1f}%)"
    if error_tables > 0:
        summary_text += f" | ❌ {error_tables} error(s)"
    
    # Add summary as table caption
    table.caption = summary_text
    table.caption_style = "bold red on white"
    
    return table


class SimpleProgressManager:
    """Simple progress manager for sequential processing with Rich Live display."""
    
    def __init__(self, show_progress: bool = True, clear_screen: bool = False, refresh_per_second: float = 1.0):
        """Initialize the simple progress manager.
        
        Args:
            show_progress: Whether to display progress tables
            clear_screen: Deprecated - kept for compatibility, Live display doesn't clear screen
            refresh_per_second: How many times per second to refresh the display
        """
        self.show_progress = show_progress
        self.clear_screen = clear_screen  # Kept for compatibility but not used
        self.refresh_per_second = refresh_per_second
        self.is_running = False
        self.console = Console() if show_progress else None
        self.last_display_time = 0.0
        self.tables: Dict[str, Dict[str, Any]] = {}
        self.completed_tables = set()
        self.live_display = None
        self.update_counter = 0  # Counter for throttling display updates
        self.update_frequency = 5  # Update display every N progress updates
        
    def start(self, table_names: List[str]) -> 'SimpleProgressManager':
        """Start progress tracking for the given tables.
        
        Args:
            table_names: List of table names to track
            
        Returns:
            Self for method chaining
        """
        self.is_running = True
        self.completed_tables = set()
        
        L.debug(f"Starting progress tracking for {len(table_names)} tables")
        
        # Initialize tables
        for table_name in table_names:
            self.tables[table_name] = {
                "registered": False,
                "started": False,
                "completed": False,
                "count": 0,
                "total": 0,
                "message": "Waiting...",
                "start_time": None,
                "error": None,
                "status": "pending"
            }
        
        # Start Live display
        if self.show_progress and self.console:
            initial_table = _create_progress_table(self.tables, time.time())
            self.live_display = Live(initial_table, console=self.console, 
                                   refresh_per_second=self.refresh_per_second, auto_refresh=True)
            self.live_display.start()
            
        return self
    
    def register_table(self, table_id: str, schema_timestamp: int = 0):
        """Register a table with placeholder total.
        
        Args:
            table_id: Unique identifier for the table
            schema_timestamp: Schema timestamp for this table
        """
        if not self.is_running:
            return
            
        if table_id not in self.tables:
            self.tables[table_id] = {
                "registered": False,
                "started": False,
                "completed": False,
                "count": 0,
                "total": 0,
                "message": "Waiting...",
                "start_time": None,
                "error": None,
                "status": "pending"
            }
        
        placeholder_total = 1  # Small placeholder total that will be updated when actual total is known
        self.tables[table_id].update({
            "registered": True,
            "total": placeholder_total,
            "message": "Registered (pending start)",
            "schema_timestamp": schema_timestamp,
            "status": "registered"
        })
        
        L.debug(f"Registered table {table_id} with placeholder total")
    
    def start_table(self, table_id: str, total_folders: int):
        """Mark a table as started with the actual total.
        
        Args:
            table_id: Unique identifier for the table
            total_folders: Total number of folders to process (mandatory)
        """
        if not self.is_running or table_id not in self.tables:
            return
            
        # Update total with actual value
        self.tables[table_id]["total"] = total_folders
        self.tables[table_id].update({
            "started": True,
            "start_time": time.time(),  # Reset start time when processing actually begins
            "message": "",
            "status": "running"
        })
        
        self._update_live_display()
        L.debug(f"Started processing table {table_id} with {total_folders} folders")
    
    def update_progress(self, table_id: str, count: int, total: int, msg: str = ""):
        """Update progress for the current table.
        
        Args:
            table_id: Unique identifier for the table
            count: Current progress count
            total: Total items to process (may update the registered total)
            msg: Optional status message
        """
        if not self.is_running or table_id not in self.tables:
            return
            
        # Update table state
        self.tables[table_id].update({
            "count": count,
            "total": total,
            "message": msg or self.tables[table_id]["message"],
            "status": "running" if count < total else "completed"
        })
        
        # Throttle display updates for better performance
        self.update_counter += 1
        if self.update_counter >= self.update_frequency:
            self._update_live_display()
            self.update_counter = 0
    
    def complete_table(self, table_id: str, error_message: Optional[str] = None):
        """Mark a table as completed.
        
        Args:
            table_id: Unique identifier for the table
            error_message: Optional error message if completion was due to error
        """
        if not self.is_running or table_id not in self.tables:
            return
            
        self.tables[table_id].update({
            "completed": True,
            "error": error_message,
            "message": error_message or "Completed",
            "status": "error" if error_message else "completed"
        })
        self.completed_tables.add(table_id)
        
        # Always update display immediately on completion (important event)
        self._update_live_display()
        L.debug(f"Completed table {table_id} with status: {'error' if error_message else 'success'}")
    
    def stop(self):
        """Stop progress tracking and clean up resources."""
        if not self.is_running:
            return
            
        self.is_running = False
        
        if self.live_display:
            # Final update
            self._update_live_display()
            time.sleep(0.5)  # Let final update display
            self.live_display.stop()
            self.live_display = None
        
        self.completed_tables.clear()
        L.debug("Stopped progress tracking")
    
    def _update_live_display(self):
        """Update the live display with current progress."""
        if self.live_display and self.show_progress:
            updated_table = _create_progress_table(self.tables, time.time())
            self.live_display.update(updated_table)
    
    def get_progress(self, table_id: str) -> Optional[Dict[str, Any]]:
        """Get progress information for a specific table.
        
        Args:
            table_id: Unique identifier for the table
            
        Returns:
            Dictionary with progress information or None if not found
        """
        return self.tables.get(table_id)
    
    def get_all_progress(self) -> Dict[str, Dict[str, Any]]:
        """Get progress information for all tables.
        
        Returns:
            Dictionary mapping table_id to progress information
        """
        return self.tables.copy()
    
    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of all progress.
        
        Returns:
            Dictionary with summary statistics
        """
        total_tables = len(self.tables)
        completed_tables = sum(1 for table in self.tables.values() if table["completed"])
        error_tables = sum(1 for table in self.tables.values() if table["error"])
        
        total_items = sum(table["total"] for table in self.tables.values())
        completed_items = sum(table["count"] for table in self.tables.values())
        
        return {
            "total_tables": total_tables,
            "completed_tables": completed_tables,
            "error_tables": error_tables,
            "total_items": total_items,
            "completed_items": completed_items,
            "completion_percentage": (completed_items / total_items * 100) if total_items > 0 else 0,
            "is_running": self.is_running
        }
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensure cleanup."""
        self.stop()


class MultiProgressManager:
    """Progress manager for Ray-based parallel processing with Rich Live display."""
    
    def __init__(self, show_progress: bool = True, clear_screen: bool = False, refresh_per_second: float = 1.0):
        """Initialize the multi progress manager.
        
        Args:
            show_progress: Whether to display progress tables
            clear_screen: Deprecated - kept for compatibility, Live display doesn't clear screen
            refresh_per_second: How many times per second to refresh the display
        """
        self.show_progress = show_progress
        self.clear_screen = clear_screen  # Kept for compatibility but not used
        self.refresh_per_second = refresh_per_second
        self.is_running = False
        self.console = Console() if show_progress else None
        self.last_display_time = 0.0
        self.tracker_actor = None
        self.table_names = []
        self.live_display = None
        
    def start(self, table_names: List[str]) -> 'MultiProgressManager':
        """Start Ray-based progress tracking and register all tables.
        
        Args:
            table_names: List of table names to track
            
        Returns:
            Self for method chaining
        """
        if not ray.is_initialized():
            L.warning("Ray not initialized, cannot use MultiProgressManager")
            raise RuntimeError("Ray must be initialized to use MultiProgressManager")
            
        self.is_running = True
        self.table_names = table_names
        
        # Create the Ray actor for tracking progress
        self.tracker_actor = MultiProgressTracker.remote()
        
        # Register all tables upfront with placeholder totals
        for table_name in table_names:
            self.tracker_actor.register_table.remote(table_name, 0)  # Fire-and-forget
        
        # Start Live display for immediate visual feedback
        if self.show_progress and self.console:
            # Initialize with empty progress data, will be updated as tables register
            initial_tables_data = {}
            initial_table = _create_progress_table(initial_tables_data, time.time())
            self.live_display = Live(initial_table, console=self.console, 
                                   refresh_per_second=self.refresh_per_second, auto_refresh=True)
            self.live_display.start()
        
        L.debug(f"Started multi progress tracking and registered {len(table_names)} tables")
        return self
    
    def wait_for_completion(self, futures):
        """Wait for Ray futures to complete while blocking and updating progress visuals.
        
        This method blocks the main thread and periodically fetches progress data
        from the Ray actor to update the visual display until all futures are complete.
        
        Args:
            futures: List of Ray futures to wait for
            
        Returns:
            List of results from the completed futures
        """
        if not futures:
            return []
            
        if not self.show_progress:
            # If no progress display, just wait normally
            return ray.get(futures)
        
        L.debug(f"Waiting for {len(futures)} futures to complete with progress updates")
        
        # Initial display
        self._update_live_display()
        
        # Keep checking and updating progress until all futures are done
        remaining_futures = futures[:]
        last_update = time.time()
        
        while remaining_futures:
            # Check if any futures are ready with a short timeout
            ready_futures, remaining_futures = ray.wait(remaining_futures, timeout=0.1)
            
            current_time = time.time()
            
            # Update display more frequently for live updates (every 0.5 seconds or when futures complete)
            if (current_time - last_update >= 0.5) or ready_futures:
                self._update_live_display()
                last_update = current_time
            
            # Additional check: if all tables are completed in the actor, break out
            # This handles the intermittent case where futures don't get marked as ready
            # but all actual work is done
            try:
                if self.tracker_actor and ray.get(self.tracker_actor.is_all_complete.remote(), timeout=0.5):
                    L.debug("All tables completed in actor - breaking from wait loop")
                    break
            except (ray.exceptions.GetTimeoutError, Exception) as e:
                L.debug(f"Could not check actor completion status: {e}")
            
            # Small sleep to prevent busy waiting, but keep it short for responsive updates
            if remaining_futures:
                time.sleep(1.0)
        
        # Final display update
        self._update_live_display()
        
        # Get all results (should be immediate since all futures are done)
        try:
            results = ray.get(futures)
            L.debug(f"All {len(futures)} futures completed successfully")
            return results
        except Exception as e:
            L.error(f"Error getting Ray results: {e}")
            return [None] * len(futures)
    
    
    def stop(self):
        """Stop progress tracking and clean up."""
        if not self.is_running:
            return
            
        self.is_running = False
        
        # Final progress display and cleanup
        if self.live_display:
            self._update_live_display()
            time.sleep(0.5)  # Let final update display
            self.live_display.stop()
            self.live_display = None
            
        L.debug("Stopped multi progress tracking")
    
    def _update_live_display(self):
        """Update the live display with current progress from Ray actor."""
        if not self.live_display or not self.show_progress or not self.tracker_actor:
            return
            
        try:
            # Get progress data from Ray actor
            progress_data = ray.get(self.tracker_actor.get_all_progress.remote(), timeout=1.0)
            # Convert to display format
            tables_data = {}
            for table_id, (count, total, msg, status, start_time) in progress_data.items():
                tables_data[table_id] = {
                    "count": count,
                    "total": total,
                    "message": msg,
                    "status": status,
                    "start_time": start_time,
                    "completed": status in ["completed", "error"],
                    "error": msg if status == "error" else None
                }
            
            # Update the live display
            updated_table = _create_progress_table(tables_data, time.time())
            self.live_display.update(updated_table)
            
        except ray.exceptions.GetTimeoutError:
            L.debug("Ray actor progress fetch timed out")
        except Exception as e:
            L.debug(f"Error updating live display: {e}")
    
    def get_tracker_actor(self):
        """Get the underlying Ray actor for direct access if needed.
        
        Returns:
            Ray actor instance or None if not available
        """
        return self.tracker_actor
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensure cleanup."""
        self.stop()

