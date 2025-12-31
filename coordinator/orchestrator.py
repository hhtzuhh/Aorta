#!/usr/bin/env python3
"""
Orchestrator - Unified Producer Coordination
Uses a single Kafka connection to avoid DNS overload
"""

import sys
import time
import argparse
import requests
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from producers.unified_producer import UnifiedProducer


def wait_for_clock_service(clock_url: str, timeout: int = 30):
    """Wait for clock service to be ready"""
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
    """Start auto-tick on clock service"""
    response = requests.post(
        f"{clock_url}/start",
        json={"interval_seconds": tick_interval},
        timeout=5
    )
    response.raise_for_status()
    print(f"⏰ Clock auto-tick started (every {tick_interval}s)")


def stop_clock(clock_url: str):
    """Stop auto-tick on clock service"""
    try:
        response = requests.post(f"{clock_url}/stop", timeout=5)
        response.raise_for_status()
        print("🛑 Clock auto-tick stopped")
    except Exception as e:
        print(f"⚠️  Failed to stop clock: {e}")


def print_header():
    print("=" * 80)
    print("🏥 AORTA - UNIFIED STREAMING ORCHESTRATOR")
    print("=" * 80)
    print()


def print_tick_summary(tick_num: int, window_start: str, window_end: str, counts: dict):
    """Print summary of a tick cycle"""
    print(f"\n{'─' * 80}")
    print(f"⏰ Tick #{tick_num:04d}: {window_start} → {window_end}")
    print(f"{'─' * 80}")

    if counts['admissions'] > 0:
        print(f"   🏥 Admissions: {counts['admissions']} events")
    if counts['labs'] > 0:
        print(f"   🔬 Labs: {counts['labs']} events")
    if counts['vitals'] > 0:
        print(f"   ❤️  Vitals: {counts['vitals']} events")
    if counts['icu'] > 0:
        print(f"   🚨 ICU Admissions: {counts['icu']} events")

    total = sum(counts.values())
    if total == 0:
        print(f"   📭 No events in this window")


def main():
    parser = argparse.ArgumentParser(
        description='Unified producer streaming with simulation clock'
    )
    parser.add_argument(
        '--subject-ids',
        type=int,
        nargs='+',
        required=True,
        help='Patient IDs to filter by (required, e.g., --subject-ids 10003400 10006701)'
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
    parser.add_argument(
        '--start-time',
        type=str,
        help='Simulation start time (YYYY-MM-DD HH:MM:SS)'
    )
    parser.add_argument(
        '--tick-minutes',
        type=int,
        default=10,
        help='Minutes per tick window (default: 10)'
    )
    parser.add_argument(
        '--skip-clock-wait',
        action='store_true',
        help='Skip waiting for clock service (use when clock is embedded)'
    )

    args = parser.parse_args()

    print_header()

    # Configuration
    print("📋 Configuration:")
    print(f"   Clock URL: {args.clock_url}")
    print(f"   Tick interval: {args.tick_interval}s")
    print(f"   Tick window size: {args.tick_minutes} minutes")
    if args.start_time:
        print(f"   Start time: {args.start_time}")
    print(f"   Patient filter: {', '.join(map(str, args.subject_ids))}")
    if args.max_ticks:
        print(f"   Max ticks: {args.max_ticks}")
    print()

    # Wait for clock service (unless skipped for embedded mode)
    if not args.skip_clock_wait:
        try:
            wait_for_clock_service(args.clock_url)
        except TimeoutError as e:
            print(f"❌ Error: {e}")
            print("\n💡 Tip: Start the clock service first:")
            print("   cd Aorta/coordinator")
            print("   uvicorn main:app --reload --port 9000")
            return 1
    else:
        print("⏭️  Skipping clock service wait (embedded mode)")

    # Reset clock if start time provided (skip in embedded mode - backend already reset it)
    if args.start_time and not args.skip_clock_wait:
        print(f"\n⏰ Resetting clock to start time: {args.start_time}")
        try:
            response = requests.post(
                f"{args.clock_url}/reset",
                json={
                    "start_time": args.start_time,
                    "tick_minutes": args.tick_minutes,
                    "tick_interval_seconds": args.tick_interval
                },
                timeout=5
            )
            response.raise_for_status()
            print("   ✅ Clock reset successful")
        except Exception as e:
            print(f"   ❌ Failed to reset clock: {e}")
            return 1
    elif args.start_time and args.skip_clock_wait:
        print(f"\n⏭️  Skipping clock reset (backend already reset to {args.start_time})")

    # Initialize unified producer
    print("\n🔧 Initializing unified producer...")
    try:
        producer = UnifiedProducer(
            clock_url=args.clock_url,
            subject_ids=args.subject_ids
        )
        if not producer.warm_up():
            print("   ⚠️  Producer warm-up failed, continuing anyway...")
    except Exception as e:
        print(f"❌ Failed to initialize producer: {e}")
        return 1

    print("\n✅ Producer initialized")

    # Start clock (skip in embedded mode - backend already started it)
    if not args.skip_clock_wait:
        try:
            start_clock(args.clock_url, args.tick_interval)
        except Exception as e:
            print(f"❌ Failed to start clock: {e}")
            producer.close()
            return 1
    else:
        print("⏭️  Skipping clock start (backend already started it)")
        print("   Producer will fetch time windows from clock API")

    # Main loop
    print("\n🚀 Starting stream processing...")
    print("   Press Ctrl+C to stop\n")

    tick_count = 0
    totals = {'admissions': 0, 'labs': 0, 'icu': 0, 'vitals': 0}

    try:
        while True:
            tick_count += 1

            # Get current window
            try:
                response = requests.get(f"{args.clock_url}/current", timeout=15)
                response.raise_for_status()
                window = response.json()
                window_start = window["window_start"]
                window_end = window["window_end"]
            except Exception as e:
                print(f"⚠️  Failed to get current window: {e}")
                time.sleep(args.tick_interval)
                continue

            # Process tick
            try:
                counts = producer.process_tick()
            except Exception as e:
                print(f"⚠️  Error processing tick: {e}")
                time.sleep(args.tick_interval)
                continue

            # Update totals
            for key in totals:
                totals[key] += counts[key]

            # Print summary
            print_tick_summary(tick_count, window_start, window_end, counts)

            # Check max ticks
            if args.max_ticks and tick_count >= args.max_ticks:
                print(f"\n✅ Reached max ticks ({args.max_ticks})")
                break

            time.sleep(args.tick_interval)

    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")

    finally:
        print("\n🧹 Cleaning up...")
        stop_clock(args.clock_url)
        print("⏳ Flushing producer...")
        producer.flush()
        producer.close()

        # Final summary
        print("\n" + "=" * 80)
        print("📊 FINAL SUMMARY")
        print("=" * 80)
        print(f"Total ticks processed: {tick_count}")
        print(f"Total admissions: {totals['admissions']}")
        print(f"Total labs: {totals['labs']}")
        print(f"Total vitals: {totals['vitals']}")
        print(f"Total ICU admissions: {totals['icu']}")
        print(f"Total events: {sum(totals.values())}")
        print("\n✅ Orchestrator shutdown complete")

    return 0


if __name__ == "__main__":
    sys.exit(main())
