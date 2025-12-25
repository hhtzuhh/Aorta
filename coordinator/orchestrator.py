#!/usr/bin/env python3
"""
Orchestrator - Multi-Producer Coordination
Coordinates multiple time-aware producers with the simulation clock
"""

import sys
import time
import argparse
import requests
from pathlib import Path

# Add parent directory to path to import producers
sys.path.insert(0, str(Path(__file__).parent.parent))

from producers.stream_admissions_coordinated import AdmissionProducer
from producers.stream_labs import LabProducer


def wait_for_clock_service(clock_url: str, timeout: int = 30):
    """
    Wait for the clock service to be ready.

    Args:
        clock_url: URL of the clock service
        timeout: Maximum seconds to wait

    Raises:
        TimeoutError: If clock service doesn't become ready in time
    """
    print(f"⏳ Waiting for clock service at {clock_url}...")
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{clock_url}/status", timeout=2)
            if response.status_code == 200:
                print(f"✅ Clock service is ready")
                return
        except requests.exceptions.RequestException:
            pass

        time.sleep(1)

    raise TimeoutError(f"Clock service at {clock_url} did not become ready in {timeout}s")


def start_clock(clock_url: str, tick_interval: float):
    """
    Start the auto-tick on the clock service.

    Args:
        clock_url: URL of the clock service
        tick_interval: Seconds between ticks

    Raises:
        Exception: If clock fails to start
    """
    response = requests.post(
        f"{clock_url}/start",
        json={"interval_seconds": tick_interval},
        timeout=5
    )
    response.raise_for_status()
    print(f"⏰ Clock auto-tick started (every {tick_interval}s)")


def stop_clock(clock_url: str):
    """
    Stop the auto-tick on the clock service.

    Args:
        clock_url: URL of the clock service
    """
    try:
        response = requests.post(f"{clock_url}/stop", timeout=5)
        response.raise_for_status()
        print("🛑 Clock auto-tick stopped")
    except Exception as e:
        print(f"⚠️  Failed to stop clock: {e}")


def print_header():
    """Print orchestrator header"""
    print("=" * 80)
    print("🏥 AORTA - MULTI-PRODUCER STREAMING ORCHESTRATOR")
    print("=" * 80)
    print()


def print_tick_summary(tick_num: int, window_start: str, window_end: str,
                       admission_count: int, lab_count: int):
    """
    Print summary of a tick cycle.

    Args:
        tick_num: Tick number
        window_start: Window start time
        window_end: Window end time
        admission_count: Number of admissions processed
        lab_count: Number of labs processed
    """
    print(f"\n{'─' * 80}")
    print(f"⏰ Tick #{tick_num:04d}: {window_start} → {window_end}")
    print(f"{'─' * 80}")

    if admission_count > 0:
        print(f"   🏥 Admissions: {admission_count} events")
    if lab_count > 0:
        print(f"   🔬 Labs: {lab_count} events")

    if admission_count == 0 and lab_count == 0:
        print(f"   📭 No events in this window")


def main():
    """Main orchestrator function"""

    parser = argparse.ArgumentParser(
        description='Coordinate multi-producer streaming with simulation clock'
    )
    parser.add_argument(
        '--subject-id',
        type=int,
        help='Filter by patient ID (for development/testing)'
    )
    parser.add_argument(
        '--tick-interval',
        type=float,
        default=2.0,
        help='Seconds between clock ticks (default: 2.0)'
    )
    parser.add_argument(
        '--max-ticks',
        type=int,
        help='Maximum number of ticks to process (default: unlimited)'
    )
    parser.add_argument(
        '--clock-url',
        type=str,
        default='http://localhost:9000',
        help='URL of the clock service (default: http://localhost:9000)'
    )

    args = parser.parse_args()

    print_header()

    # Configuration
    print("📋 Configuration:")
    print(f"   Clock URL: {args.clock_url}")
    print(f"   Tick interval: {args.tick_interval}s")
    if args.subject_id:
        print(f"   Patient filter: {args.subject_id}")
    else:
        print(f"   Patient filter: ALL patients")
    if args.max_ticks:
        print(f"   Max ticks: {args.max_ticks}")
    print()

    # Wait for clock service
    try:
        wait_for_clock_service(args.clock_url)
    except TimeoutError as e:
        print(f"❌ Error: {e}")
        print("\n💡 Tip: Start the clock service first:")
        print("   cd Aorta/coordinator")
        print("   uvicorn main:app --reload --port 9000")
        return 1

    # Initialize producers
    print("\n🔧 Initializing producers...")
    try:
        admission_producer = AdmissionProducer(
            clock_url=args.clock_url,
            subject_id=args.subject_id
        )
        lab_producer = LabProducer(
            clock_url=args.clock_url,
            subject_id=args.subject_id
        )
    except Exception as e:
        print(f"❌ Failed to initialize producers: {e}")
        return 1

    print("\n✅ All producers initialized")

    # Start clock auto-tick
    try:
        start_clock(args.clock_url, args.tick_interval)
    except Exception as e:
        print(f"❌ Failed to start clock: {e}")
        admission_producer.close()
        lab_producer.close()
        return 1

    # Main processing loop
    print("\n🚀 Starting stream processing...")
    print("   Press Ctrl+C to stop\n")

    tick_count = 0
    total_admissions = 0
    total_labs = 0

    try:
        while True:
            # Wait for next tick
            time.sleep(args.tick_interval)
            tick_count += 1

            # Get current window from clock
            try:
                response = requests.get(f"{args.clock_url}/current", timeout=5)
                response.raise_for_status()
                window = response.json()
                window_start = window["window_start"]
                window_end = window["window_end"]
            except Exception as e:
                print(f"⚠️  Failed to get current window: {e}")
                continue

            # Process tick for each producer
            try:
                admission_count = admission_producer.process_tick()
                lab_count = lab_producer.process_tick()
            except Exception as e:
                print(f"⚠️  Error processing tick: {e}")
                continue

            # Update totals
            total_admissions += admission_count
            total_labs += lab_count

            # Print summary
            print_tick_summary(
                tick_count,
                window_start,
                window_end,
                admission_count,
                lab_count
            )

            # Check if we've hit max ticks
            if args.max_ticks and tick_count >= args.max_ticks:
                print(f"\n✅ Reached max ticks ({args.max_ticks})")
                break

    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")

    finally:
        # Cleanup
        print("\n🧹 Cleaning up...")
        stop_clock(args.clock_url)

        print("⏳ Flushing producers...")
        admission_producer.flush()
        lab_producer.flush()

        admission_producer.close()
        lab_producer.close()

        # Print final summary
        print("\n" + "=" * 80)
        print("📊 FINAL SUMMARY")
        print("=" * 80)
        print(f"Total ticks processed: {tick_count}")
        print(f"Total admissions streamed: {total_admissions}")
        print(f"Total labs streamed: {total_labs}")
        print(f"Total events: {total_admissions + total_labs}")
        print("\n✅ Orchestrator shutdown complete")

    return 0


if __name__ == "__main__":
    sys.exit(main())
