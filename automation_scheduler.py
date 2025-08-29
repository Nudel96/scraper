#!/usr/bin/env python3
"""
Automation Scheduler for Trading Heatmap System
===============================================

Handles automated data collection, processing, and synchronization.
Includes error handling, logging, and monitoring capabilities.

Usage:
    python automation_scheduler.py [--config scheduler_config.yaml] [--daemon]
"""

import asyncio
import logging
import signal
import sys
import time
import yaml
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import subprocess
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('automation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class JobConfig:
    """Configuration for a scheduled job"""
    name: str
    command: str
    schedule: str  # cron-like: "*/5 * * * *" (every 5 minutes)
    enabled: bool = True
    timeout: int = 300  # 5 minutes default
    retry_count: int = 3
    retry_delay: int = 60  # seconds
    environment: Dict[str, str] = None

@dataclass
class JobResult:
    """Result of a job execution"""
    job_name: str
    start_time: datetime
    end_time: datetime
    success: bool
    exit_code: int
    stdout: str
    stderr: str
    error_message: Optional[str] = None

class CronParser:
    """Simple cron expression parser"""
    
    @staticmethod
    def parse_schedule(schedule: str) -> Dict[str, Any]:
        """Parse cron schedule string"""
        parts = schedule.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron schedule: {schedule}")
        
        return {
            'minute': parts[0],
            'hour': parts[1],
            'day': parts[2],
            'month': parts[3],
            'weekday': parts[4]
        }
    
    @staticmethod
    def should_run(schedule: str, current_time: datetime) -> bool:
        """Check if job should run at current time"""
        try:
            parsed = CronParser.parse_schedule(schedule)
            
            # Simple implementation - supports * and specific values
            def matches(cron_field: str, time_value: int) -> bool:
                if cron_field == '*':
                    return True
                if '/' in cron_field:
                    # Handle */N syntax
                    if cron_field.startswith('*/'):
                        interval = int(cron_field[2:])
                        return time_value % interval == 0
                if ',' in cron_field:
                    # Handle comma-separated values
                    values = [int(v.strip()) for v in cron_field.split(',')]
                    return time_value in values
                # Single value
                return int(cron_field) == time_value
            
            return (
                matches(parsed['minute'], current_time.minute) and
                matches(parsed['hour'], current_time.hour) and
                matches(parsed['day'], current_time.day) and
                matches(parsed['month'], current_time.month) and
                matches(parsed['weekday'], current_time.weekday())
            )
        except Exception as e:
            logger.error(f"Error parsing schedule '{schedule}': {e}")
            return False

class JobExecutor:
    """Executes scheduled jobs with error handling and retries"""
    
    def __init__(self, max_workers: int = 4):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running_jobs = {}
    
    async def execute_job(self, job_config: JobConfig) -> JobResult:
        """Execute a job with timeout and retry logic"""
        logger.info(f"Starting job: {job_config.name}")
        start_time = datetime.now()
        
        for attempt in range(job_config.retry_count + 1):
            try:
                if attempt > 0:
                    logger.info(f"Retrying job {job_config.name} (attempt {attempt + 1})")
                    await asyncio.sleep(job_config.retry_delay)
                
                # Execute command
                result = await self._run_command(job_config)
                
                if result.success:
                    logger.info(f"Job {job_config.name} completed successfully")
                    return result
                else:
                    logger.warning(f"Job {job_config.name} failed (attempt {attempt + 1}): {result.error_message}")
                    if attempt == job_config.retry_count:
                        return result
                        
            except Exception as e:
                error_msg = f"Exception in job {job_config.name}: {str(e)}"
                logger.error(error_msg)
                if attempt == job_config.retry_count:
                    return JobResult(
                        job_name=job_config.name,
                        start_time=start_time,
                        end_time=datetime.now(),
                        success=False,
                        exit_code=-1,
                        stdout="",
                        stderr="",
                        error_message=error_msg
                    )
    
    async def _run_command(self, job_config: JobConfig) -> JobResult:
        """Run a single command"""
        start_time = datetime.now()
        
        try:
            # Prepare environment
            env = dict(os.environ)
            if job_config.environment:
                env.update(job_config.environment)
            
            # Execute command
            process = await asyncio.create_subprocess_shell(
                job_config.command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env
            )
            
            # Wait with timeout
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=job_config.timeout
                )
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                raise Exception(f"Job timed out after {job_config.timeout} seconds")
            
            end_time = datetime.now()
            success = process.returncode == 0
            
            return JobResult(
                job_name=job_config.name,
                start_time=start_time,
                end_time=end_time,
                success=success,
                exit_code=process.returncode,
                stdout=stdout.decode('utf-8') if stdout else "",
                stderr=stderr.decode('utf-8') if stderr else "",
                error_message=None if success else f"Command failed with exit code {process.returncode}"
            )
            
        except Exception as e:
            return JobResult(
                job_name=job_config.name,
                start_time=start_time,
                end_time=datetime.now(),
                success=False,
                exit_code=-1,
                stdout="",
                stderr="",
                error_message=str(e)
            )

class AutomationScheduler:
    """Main scheduler for automated tasks"""
    
    def __init__(self, config_path: str = "scheduler_config.yaml"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.executor = JobExecutor(max_workers=self.config.get('max_workers', 4))
        self.running = False
        self.job_history = []
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _load_config(self) -> Dict[str, Any]:
        """Load scheduler configuration"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f"Loaded scheduler config from {self.config_path}")
            return config
        except FileNotFoundError:
            logger.warning(f"Config file not found: {self.config_path}, using defaults")
            return self._default_config()
        except yaml.YAMLError as e:
            logger.error(f"Error parsing config: {e}")
            raise
    
    def _default_config(self) -> Dict[str, Any]:
        """Default configuration"""
        return {
            'check_interval': 60,  # Check every minute
            'max_workers': 4,
            'jobs': [
                {
                    'name': 'scraper_data_collection',
                    'command': 'python scraper/main.py',
                    'schedule': '0 */6 * * *',  # Every 6 hours
                    'enabled': True,
                    'timeout': 600,
                    'retry_count': 2
                },
                {
                    'name': 'bridge_sync',
                    'command': 'python bridge_scraper_to_backend.py',
                    'schedule': '15 */6 * * *',  # 15 minutes after scraper
                    'enabled': True,
                    'timeout': 300,
                    'retry_count': 3
                },
                {
                    'name': 'backend_score_recompute',
                    'command': 'curl -X POST http://localhost:8000/jobs/recompute-bias',
                    'schedule': '30 */6 * * *',  # 30 minutes after scraper
                    'enabled': True,
                    'timeout': 120,
                    'retry_count': 2
                }
            ]
        }
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    def _parse_jobs(self) -> List[JobConfig]:
        """Parse job configurations"""
        jobs = []
        for job_data in self.config.get('jobs', []):
            try:
                job = JobConfig(
                    name=job_data['name'],
                    command=job_data['command'],
                    schedule=job_data['schedule'],
                    enabled=job_data.get('enabled', True),
                    timeout=job_data.get('timeout', 300),
                    retry_count=job_data.get('retry_count', 3),
                    retry_delay=job_data.get('retry_delay', 60),
                    environment=job_data.get('environment', {})
                )
                jobs.append(job)
            except KeyError as e:
                logger.error(f"Invalid job configuration, missing key: {e}")
        
        return jobs
    
    async def run_scheduler(self):
        """Main scheduler loop"""
        logger.info("Starting automation scheduler")
        self.running = True
        jobs = self._parse_jobs()
        
        logger.info(f"Loaded {len(jobs)} jobs:")
        for job in jobs:
            status = "enabled" if job.enabled else "disabled"
            logger.info(f"  - {job.name}: {job.schedule} ({status})")
        
        check_interval = self.config.get('check_interval', 60)
        
        while self.running:
            try:
                current_time = datetime.now()
                
                # Check which jobs should run
                for job in jobs:
                    if not job.enabled:
                        continue
                    
                    if CronParser.should_run(job.schedule, current_time):
                        # Avoid running the same job multiple times
                        if job.name not in self.executor.running_jobs:
                            logger.info(f"Scheduling job: {job.name}")
                            task = asyncio.create_task(self.executor.execute_job(job))
                            self.executor.running_jobs[job.name] = task
                            
                            # Add callback to clean up and log result
                            task.add_done_callback(
                                lambda t, job_name=job.name: self._job_completed(job_name, t)
                            )
                
                # Wait before next check
                await asyncio.sleep(check_interval)
                
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                await asyncio.sleep(check_interval)
        
        logger.info("Scheduler stopped")
    
    def _job_completed(self, job_name: str, task: asyncio.Task):
        """Handle job completion"""
        try:
            result = task.result()
            self.job_history.append(result)
            
            # Keep only last 100 results
            if len(self.job_history) > 100:
                self.job_history = self.job_history[-100:]
            
            # Log result
            if result.success:
                logger.info(f"Job {job_name} completed successfully in {result.end_time - result.start_time}")
            else:
                logger.error(f"Job {job_name} failed: {result.error_message}")
                if result.stderr:
                    logger.error(f"Job {job_name} stderr: {result.stderr}")
            
        except Exception as e:
            logger.error(f"Error handling job completion for {job_name}: {e}")
        finally:
            # Remove from running jobs
            self.executor.running_jobs.pop(job_name, None)
    
    def get_status(self) -> Dict[str, Any]:
        """Get scheduler status"""
        return {
            'running': self.running,
            'active_jobs': list(self.executor.running_jobs.keys()),
            'recent_results': [
                {
                    'job_name': r.job_name,
                    'start_time': r.start_time.isoformat(),
                    'success': r.success,
                    'duration': (r.end_time - r.start_time).total_seconds()
                }
                for r in self.job_history[-10:]  # Last 10 results
            ]
        }

async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Automation Scheduler")
    parser.add_argument("--config", default="scheduler_config.yaml", help="Config file path")
    parser.add_argument("--daemon", action="store_true", help="Run as daemon")
    parser.add_argument("--status", action="store_true", help="Show status and exit")
    
    args = parser.parse_args()
    
    scheduler = AutomationScheduler(args.config)
    
    if args.status:
        status = scheduler.get_status()
        print(json.dumps(status, indent=2))
        return
    
    if args.daemon:
        logger.info("Running in daemon mode")
    
    try:
        await scheduler.run_scheduler()
    except KeyboardInterrupt:
        logger.info("Scheduler interrupted by user")
    except Exception as e:
        logger.error(f"Scheduler error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    import os
    exit(asyncio.run(main()))
