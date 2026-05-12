"""
Example: Python worker connecting to a rust-pipe dispatcher

Run the Rust dispatcher first, then:
    python examples/python_worker.py
"""
import asyncio
from rust_pipe import create_worker, Task


async def scan_target(task: Task):
    """Handle security scanning tasks."""
    print(f"Scanning {task.payload['url']} with checks: {task.payload['checks']}")

    # Simulate scanning work
    await asyncio.sleep(2)

    return {
        "vulnerabilities": [
            {
                "type": "sqli",
                "severity": "critical",
                "url": f"{task.payload['url']}/api/users?id=1 OR 1=1",
                "description": "SQL injection in user lookup endpoint",
            }
        ],
        "scanned_endpoints": 38,
        "duration": "2.0s",
    }


async def analyze_dependencies(task: Task):
    """Handle dependency analysis tasks."""
    print(f"Analyzing dependencies for: {task.payload['repository']}")
    await asyncio.sleep(3)

    return {
        "vulnerable_packages": 2,
        "total_packages": 145,
        "findings": [
            {"package": "lodash", "version": "4.17.20", "cve": "CVE-2021-23337"}
        ],
    }


async def main():
    worker = create_worker(
        url="ws://localhost:9876",
        handlers={
            "scan-target": scan_target,
            "analyze-dependencies": analyze_dependencies,
        },
        max_concurrency=5,
    )

    print("Python worker connecting to rust-pipe dispatcher...")
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
