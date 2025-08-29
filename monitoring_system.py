#!/usr/bin/env python3
"""
Monitoring System for Trading Heatmap
=====================================

Monitors system health, job performance, and data quality.
Provides alerting and reporting capabilities.

Usage:
    python monitoring_system.py [--config monitoring_config.yaml] [--dashboard]
"""

import asyncio
import logging
import json
import time
import psutil
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from pathlib import Path
import sqlite3
import yaml

logger = logging.getLogger(__name__)

@dataclass
class HealthMetric:
    """Health metric data point"""
    name: str
    value: float
    unit: str
    timestamp: datetime
    status: str  # OK, WARNING, CRITICAL
    threshold_warning: Optional[float] = None
    threshold_critical: Optional[float] = None

@dataclass
class SystemAlert:
    """System alert"""
    id: str
    severity: str  # INFO, WARNING, ERROR, CRITICAL
    component: str
    message: str
    timestamp: datetime
    resolved: bool = False
    resolution_time: Optional[datetime] = None

class HealthChecker:
    """Performs various health checks"""
    
    def __init__(self):
        self.checks = {
            'system_cpu': self.check_cpu_usage,
            'system_memory': self.check_memory_usage,
            'system_disk': self.check_disk_usage,
            'backend_api': self.check_backend_api,
            'database_connection': self.check_database,
            'scraper_data_freshness': self.check_data_freshness,
        }
    
    async def run_all_checks(self) -> List[HealthMetric]:
        """Run all health checks"""
        metrics = []
        
        for check_name, check_func in self.checks.items():
            try:
                metric = await check_func()
                if metric:
                    metrics.append(metric)
            except Exception as e:
                logger.error(f"Health check {check_name} failed: {e}")
                metrics.append(HealthMetric(
                    name=check_name,
                    value=-1,
                    unit="error",
                    timestamp=datetime.now(),
                    status="CRITICAL"
                ))
        
        return metrics
    
    async def check_cpu_usage(self) -> HealthMetric:
        """Check CPU usage"""
        cpu_percent = psutil.cpu_percent(interval=1)
        
        status = "OK"
        if cpu_percent > 90:
            status = "CRITICAL"
        elif cpu_percent > 75:
            status = "WARNING"
        
        return HealthMetric(
            name="system_cpu",
            value=cpu_percent,
            unit="percent",
            timestamp=datetime.now(),
            status=status,
            threshold_warning=75.0,
            threshold_critical=90.0
        )
    
    async def check_memory_usage(self) -> HealthMetric:
        """Check memory usage"""
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        
        status = "OK"
        if memory_percent > 95:
            status = "CRITICAL"
        elif memory_percent > 85:
            status = "WARNING"
        
        return HealthMetric(
            name="system_memory",
            value=memory_percent,
            unit="percent",
            timestamp=datetime.now(),
            status=status,
            threshold_warning=85.0,
            threshold_critical=95.0
        )
    
    async def check_disk_usage(self) -> HealthMetric:
        """Check disk usage"""
        disk = psutil.disk_usage('/')
        disk_percent = (disk.used / disk.total) * 100
        
        status = "OK"
        if disk_percent > 95:
            status = "CRITICAL"
        elif disk_percent > 85:
            status = "WARNING"
        
        return HealthMetric(
            name="system_disk",
            value=disk_percent,
            unit="percent",
            timestamp=datetime.now(),
            status=status,
            threshold_warning=85.0,
            threshold_critical=95.0
        )
    
    async def check_backend_api(self) -> HealthMetric:
        """Check backend API health"""
        try:
            start_time = time.time()
            response = requests.get("http://localhost:8000/health", timeout=10)
            response_time = (time.time() - start_time) * 1000  # ms
            
            status = "OK"
            if response.status_code != 200:
                status = "CRITICAL"
            elif response_time > 5000:  # 5 seconds
                status = "WARNING"
            elif response_time > 2000:  # 2 seconds
                status = "WARNING"
            
            return HealthMetric(
                name="backend_api_response_time",
                value=response_time,
                unit="ms",
                timestamp=datetime.now(),
                status=status,
                threshold_warning=2000.0,
                threshold_critical=5000.0
            )
            
        except Exception as e:
            logger.error(f"Backend API check failed: {e}")
            return HealthMetric(
                name="backend_api_response_time",
                value=-1,
                unit="ms",
                timestamp=datetime.now(),
                status="CRITICAL"
            )
    
    async def check_database(self) -> HealthMetric:
        """Check database connectivity"""
        try:
            # Check SQLite scraper database
            conn = sqlite3.connect("scraper/events.db", timeout=5)
            cursor = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            table_count = cursor.fetchone()[0]
            conn.close()
            
            status = "OK" if table_count > 0 else "WARNING"
            
            return HealthMetric(
                name="database_tables",
                value=table_count,
                unit="count",
                timestamp=datetime.now(),
                status=status
            )
            
        except Exception as e:
            logger.error(f"Database check failed: {e}")
            return HealthMetric(
                name="database_tables",
                value=-1,
                unit="count",
                timestamp=datetime.now(),
                status="CRITICAL"
            )
    
    async def check_data_freshness(self) -> HealthMetric:
        """Check how fresh the scraped data is"""
        try:
            conn = sqlite3.connect("scraper/events.db")
            cursor = conn.execute("""
                SELECT MAX(release_time_utc) FROM events 
                WHERE release_time_utc IS NOT NULL
            """)
            result = cursor.fetchone()
            conn.close()
            
            if result[0]:
                latest_data = datetime.fromisoformat(result[0].replace('Z', '+00:00'))
                hours_old = (datetime.now(latest_data.tzinfo) - latest_data).total_seconds() / 3600
                
                status = "OK"
                if hours_old > 48:  # 2 days
                    status = "CRITICAL"
                elif hours_old > 24:  # 1 day
                    status = "WARNING"
                
                return HealthMetric(
                    name="data_freshness",
                    value=hours_old,
                    unit="hours",
                    timestamp=datetime.now(),
                    status=status,
                    threshold_warning=24.0,
                    threshold_critical=48.0
                )
            else:
                return HealthMetric(
                    name="data_freshness",
                    value=-1,
                    unit="hours",
                    timestamp=datetime.now(),
                    status="CRITICAL"
                )
                
        except Exception as e:
            logger.error(f"Data freshness check failed: {e}")
            return HealthMetric(
                name="data_freshness",
                value=-1,
                unit="hours",
                timestamp=datetime.now(),
                status="CRITICAL"
            )

class AlertManager:
    """Manages alerts and notifications"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.active_alerts = {}
        self.alert_history = []
    
    def process_metrics(self, metrics: List[HealthMetric]) -> List[SystemAlert]:
        """Process metrics and generate alerts"""
        new_alerts = []
        
        for metric in metrics:
            alert_id = f"{metric.name}_{metric.status}"
            
            if metric.status in ["WARNING", "CRITICAL"]:
                if alert_id not in self.active_alerts:
                    # New alert
                    alert = SystemAlert(
                        id=alert_id,
                        severity=metric.status,
                        component=metric.name,
                        message=f"{metric.name} is {metric.status}: {metric.value} {metric.unit}",
                        timestamp=metric.timestamp
                    )
                    self.active_alerts[alert_id] = alert
                    new_alerts.append(alert)
                    logger.warning(f"New alert: {alert.message}")
            else:
                # Check if we need to resolve an existing alert
                if alert_id.replace("_OK", "_WARNING") in self.active_alerts:
                    resolved_alert = self.active_alerts.pop(alert_id.replace("_OK", "_WARNING"))
                    resolved_alert.resolved = True
                    resolved_alert.resolution_time = metric.timestamp
                    self.alert_history.append(resolved_alert)
                    logger.info(f"Resolved alert: {resolved_alert.message}")
                
                if alert_id.replace("_OK", "_CRITICAL") in self.active_alerts:
                    resolved_alert = self.active_alerts.pop(alert_id.replace("_OK", "_CRITICAL"))
                    resolved_alert.resolved = True
                    resolved_alert.resolution_time = metric.timestamp
                    self.alert_history.append(resolved_alert)
                    logger.info(f"Resolved alert: {resolved_alert.message}")
        
        return new_alerts
    
    async def send_notifications(self, alerts: List[SystemAlert]):
        """Send notifications for new alerts"""
        if not self.config.get('notifications', {}).get('enabled', False):
            return
        
        for alert in alerts:
            try:
                await self._send_alert_notification(alert)
            except Exception as e:
                logger.error(f"Failed to send notification for alert {alert.id}: {e}")
    
    async def _send_alert_notification(self, alert: SystemAlert):
        """Send notification for a single alert"""
        # Log notification (always enabled)
        logger.error(f"ALERT [{alert.severity}] {alert.component}: {alert.message}")
        
        # Email notification
        email_config = self.config.get('notifications', {}).get('email', {})
        if email_config.get('enabled', False):
            await self._send_email_notification(alert, email_config)
        
        # Webhook notification
        webhook_config = self.config.get('notifications', {}).get('webhook', {})
        if webhook_config.get('enabled', False):
            await self._send_webhook_notification(alert, webhook_config)
    
    async def _send_email_notification(self, alert: SystemAlert, config: Dict[str, Any]):
        """Send email notification"""
        # Email implementation would go here
        logger.info(f"Would send email notification for alert: {alert.id}")
    
    async def _send_webhook_notification(self, alert: SystemAlert, config: Dict[str, Any]):
        """Send webhook notification"""
        try:
            payload = {
                "alert_id": alert.id,
                "severity": alert.severity,
                "component": alert.component,
                "message": alert.message,
                "timestamp": alert.timestamp.isoformat()
            }
            
            response = requests.post(
                config['url'],
                json=payload,
                headers=config.get('headers', {}),
                timeout=10
            )
            response.raise_for_status()
            logger.info(f"Webhook notification sent for alert: {alert.id}")
            
        except Exception as e:
            logger.error(f"Failed to send webhook notification: {e}")

class MonitoringSystem:
    """Main monitoring system"""
    
    def __init__(self, config_path: str = "monitoring_config.yaml"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.health_checker = HealthChecker()
        self.alert_manager = AlertManager(self.config)
        self.running = False
        self.metrics_history = []
    
    def _load_config(self) -> Dict[str, Any]:
        """Load monitoring configuration"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config file not found: {self.config_path}, using defaults")
            return self._default_config()
    
    def _default_config(self) -> Dict[str, Any]:
        """Default monitoring configuration"""
        return {
            'check_interval': 300,  # 5 minutes
            'metrics_retention_hours': 24,
            'notifications': {
                'enabled': True,
                'email': {'enabled': False},
                'webhook': {'enabled': False}
            }
        }
    
    async def run_monitoring(self):
        """Main monitoring loop"""
        logger.info("Starting monitoring system")
        self.running = True
        
        check_interval = self.config.get('check_interval', 300)
        
        while self.running:
            try:
                # Run health checks
                metrics = await self.health_checker.run_all_checks()
                
                # Store metrics
                self.metrics_history.extend(metrics)
                self._cleanup_old_metrics()
                
                # Process alerts
                new_alerts = self.alert_manager.process_metrics(metrics)
                
                # Send notifications
                if new_alerts:
                    await self.alert_manager.send_notifications(new_alerts)
                
                # Log summary
                critical_count = sum(1 for m in metrics if m.status == "CRITICAL")
                warning_count = sum(1 for m in metrics if m.status == "WARNING")
                
                if critical_count > 0 or warning_count > 0:
                    logger.warning(f"Health check summary: {critical_count} critical, {warning_count} warnings")
                else:
                    logger.info("All health checks passed")
                
                await asyncio.sleep(check_interval)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(check_interval)
        
        logger.info("Monitoring system stopped")
    
    def _cleanup_old_metrics(self):
        """Remove old metrics to prevent memory buildup"""
        retention_hours = self.config.get('metrics_retention_hours', 24)
        cutoff_time = datetime.now() - timedelta(hours=retention_hours)
        
        self.metrics_history = [
            m for m in self.metrics_history 
            if m.timestamp > cutoff_time
        ]
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get data for monitoring dashboard"""
        recent_metrics = [
            m for m in self.metrics_history 
            if m.timestamp > datetime.now() - timedelta(hours=1)
        ]
        
        return {
            'timestamp': datetime.now().isoformat(),
            'system_status': self._get_overall_status(recent_metrics),
            'active_alerts': [asdict(alert) for alert in self.alert_manager.active_alerts.values()],
            'recent_metrics': [asdict(m) for m in recent_metrics[-20:]],  # Last 20 metrics
            'summary': {
                'total_metrics': len(self.metrics_history),
                'active_alerts': len(self.alert_manager.active_alerts),
                'resolved_alerts': len(self.alert_manager.alert_history)
            }
        }
    
    def _get_overall_status(self, metrics: List[HealthMetric]) -> str:
        """Determine overall system status"""
        if not metrics:
            return "UNKNOWN"
        
        latest_metrics = {}
        for metric in metrics:
            if metric.name not in latest_metrics or metric.timestamp > latest_metrics[metric.name].timestamp:
                latest_metrics[metric.name] = metric
        
        statuses = [m.status for m in latest_metrics.values()]
        
        if "CRITICAL" in statuses:
            return "CRITICAL"
        elif "WARNING" in statuses:
            return "WARNING"
        else:
            return "OK"

async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Monitoring System")
    parser.add_argument("--config", default="monitoring_config.yaml", help="Config file path")
    parser.add_argument("--dashboard", action="store_true", help="Show dashboard data and exit")
    
    args = parser.parse_args()
    
    monitoring = MonitoringSystem(args.config)
    
    if args.dashboard:
        dashboard_data = monitoring.get_dashboard_data()
        print(json.dumps(dashboard_data, indent=2, default=str))
        return
    
    try:
        await monitoring.run_monitoring()
    except KeyboardInterrupt:
        logger.info("Monitoring interrupted by user")
    except Exception as e:
        logger.error(f"Monitoring error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(asyncio.run(main()))
