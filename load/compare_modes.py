from __future__ import annotations

import argparse
import asyncio
import csv
import json
import hashlib
import platform
import subprocess
from pathlib import Path
import statistics
import time
from dataclasses import dataclass, asdict
from datetime import UTC, datetime, timedelta

import httpx


@dataclass
class RunStats:
    mode: str
    total_requests: int
    success_count: int
    conflict_count: int
    error_count: int
    avg_response_ms: float
    max_response_ms: float
    avg_success_response_ms: float = 0
    avg_conflict_response_ms: float = 0
    scenario: str = ""
    repetition: int = 0
    secondary_delay_ms: int = 0
    worker_delay_ms: int = 0
    request_batch_ms: float = 0
    completion_ms: float = 0
    worker_completed: int = 0
    worker_failed: int = 0


class CompareClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')

    async def reset_and_seed(self, client: httpx.AsyncClient) -> None:
        for endpoint in ('reset', 'seed'):
            response = await client.post(f'{self.base_url}/experiments/{endpoint}')
            response.raise_for_status()
        data = response.json()
        self.rooms = [r['id'] for r in data['rooms']]
        self.users = [u['id'] for u in data['users']]
        if len(self.rooms) != 3 or len(self.users) != 3:
            raise RuntimeError('Expected three rooms and three users')
        self.responses = []

    async def book(self, client: httpx.AsyncClient, payload: dict) -> tuple[int, float]:
        started = time.perf_counter()
        try:
            resp = await client.post(f'{self.base_url}/bookings', json=payload)
            status, body = resp.status_code, resp.json()
        except httpx.RequestError as exc:
            status, body = 0, {'error': str(exc)}
        elapsed_ms = (time.perf_counter() - started) * 1000
        self.responses.append({'status': status, 'elapsed_ms': elapsed_ms, 'body': body, 'payload': payload})
        return status, elapsed_ms


async def scenario_a(client: CompareClient, http: httpx.AsyncClient, mode: str) -> RunStats:
    base = datetime(2026, 4, 20, 8, 0, 0)
    statuses: list[int] = []
    times: list[float] = []

    for i in range(20):
        room_id = client.rooms[i % 3]
        payload = {
            'room_id': room_id,
            'user_id': client.users[(i + 1) % 3],
            'start_time': (base + timedelta(hours=i)).isoformat(),
            'end_time': (base + timedelta(hours=i + 1)).isoformat(),
            'mode': mode,
            'scenario': 'scenario_a_seq',
        }
        status, elapsed = await client.book(http, payload)
        statuses.append(status)
        times.append(elapsed)

    return build_stats(mode, statuses, times)


async def scenario_b(client: CompareClient, http: httpx.AsyncClient, mode: str) -> RunStats:
    statuses: list[int] = []
    times: list[float] = []
    base_start = datetime(2026, 4, 20, 10, 0, 0)

    async def one_call(i: int) -> tuple[int, float]:
        payload = {
            'room_id': client.rooms[0] if i < 14 else client.rooms[i % 3],
            'user_id': client.users[i % 3],
            'start_time': base_start.isoformat() if i < 14 else (base_start + timedelta(hours=i)).isoformat(),
            'end_time': (base_start + timedelta(hours=1)).isoformat() if i < 14 else (base_start + timedelta(hours=i + 1)).isoformat(),
            'mode': mode,
            'scenario': 'scenario_b_parallel',
        }
        return await client.book(http, payload)

    results = await asyncio.gather(*(one_call(i) for i in range(20)))
    for status, elapsed in results:
        statuses.append(status)
        times.append(elapsed)

    return build_stats(mode, statuses, times)


def build_stats(mode: str, statuses: list[int], times: list[float]) -> RunStats:
    success_count = sum(1 for s in statuses if 200 <= s < 300)
    conflict_count = sum(1 for s in statuses if s == 409)
    error_count = len(statuses) - success_count - conflict_count

    successful_times = [t for status, t in zip(statuses, times) if 200 <= status < 300]
    conflict_times = [t for status, t in zip(statuses, times) if status == 409]
    return RunStats(
        avg_success_response_ms=round(statistics.mean(successful_times), 3) if successful_times else 0,
        avg_conflict_response_ms=round(statistics.mean(conflict_times), 3) if conflict_times else 0,
        mode=mode,
        total_requests=len(statuses),
        success_count=success_count,
        conflict_count=conflict_count,
        error_count=error_count,
        avg_response_ms=round(statistics.mean(times), 3) if times else 0.0,
        max_response_ms=round(max(times), 3) if times else 0.0,
    )


def append_csv(path: str, rows: list[RunStats]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=list(asdict(rows[0])))
        writer.writeheader()
        writer.writerows(asdict(row) for row in rows)


async def run(base_url: str, scenario: str, output: str, repeats: int = 3, delay_ms: int = 0, secondary_delay_ms: int = 0) -> bool:
    cc = CompareClient(base_url)
    rows, runs = [], []
    async with httpx.AsyncClient(timeout=120) as client:
        for repetition in range(1, repeats + 1):
            # Alternate order to reduce systematic warm-up/order effects.
            modes = ('centralized_sync', 'hybrid_async')
            for mode in modes if repetition % 2 else modes[::-1]:
                await cc.reset_and_seed(client)
                for endpoint, payload in [('worker/on', None), ('worker/delay', {'delay_ms': delay_ms}),
                                          ('secondary/delay', {'delay_ms': secondary_delay_ms})]:
                    r = await client.post(f'{cc.base_url}/experiments/{endpoint}', json=payload)
                    r.raise_for_status()
                started = time.perf_counter()
                stats = await (scenario_a if scenario == 'a' else scenario_b)(cc, client, mode)
                stats.request_batch_ms = round((time.perf_counter() - started) * 1000, 3)
                r = await client.post(f'{cc.base_url}/experiments/drain', params={'timeout': 110})
                r.raise_for_status()
                stats.completion_ms = round((time.perf_counter() - started) * 1000, 3)
                stats.worker_completed, stats.worker_failed = r.json()['completed'], r.json()['failed']
                stats.scenario, stats.repetition = scenario, repetition
                stats.secondary_delay_ms, stats.worker_delay_ms = secondary_delay_ms, delay_ms
                metrics = await client.get(f'{cc.base_url}/experiments/metrics')
                metrics.raise_for_status()
                runs.append({'stats': asdict(stats), 'responses': cc.responses, 'metrics': metrics.json()})
                rows.append(stats)
    append_csv(output, rows)
    try:
        revision_result = subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True)
        revision = revision_result.stdout.strip() if revision_result.returncode == 0 else None
        dirty = bool(subprocess.run(['git', 'status', '--porcelain'], capture_output=True, text=True).stdout) if revision else None
    except FileNotFoundError:
        revision, dirty = None, None  # Docker/ZIP may not contain Git; file hashes still identify source.
    Path(output + '.json').write_text(json.dumps({
        'recorded_at_utc': datetime.now(UTC).replace(tzinfo=None).isoformat(), 'python': platform.python_version(),
        'platform': platform.platform(), 'revision': revision, 'working_tree_dirty': dirty,
        'base_url': base_url, 'worker_delay_ms': delay_ms, 'secondary_delay_ms': secondary_delay_ms,
        'source_sha256': {str(p).replace(chr(92), '/'): hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in sorted([*Path('app').rglob('*.py'), *Path('load').rglob('*.py'), Path('requirements.txt')])},
        'runs': runs,
    }, ensure_ascii=False, indent=2), encoding='utf-8')
    expected = (20, 0) if scenario == 'a' else (7, 13)
    valid = all((r.success_count, r.conflict_count) == expected and r.error_count == 0
                and r.worker_failed == 0 and r.worker_completed == (2*r.success_count if r.mode == 'hybrid_async' else 0)
                for r in rows)
    print(f'Results saved to {output}; expected counts verified: {valid}')
    for row in rows:
        print(row)
    return valid


def main() -> None:
    parser = argparse.ArgumentParser(description='Compare booking processing modes')
    parser.add_argument('--base-url', default='http://127.0.0.1:8000')
    parser.add_argument('--scenario', choices=['a', 'b'], default='a')
    parser.add_argument('--output', default='results.csv')
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--secondary-delay-ms', type=int, default=0)
    parser.add_argument('--worker-delay-ms', type=int, default=0)
    args = parser.parse_args()
    if args.repeats < 1 or args.worker_delay_ms < 0 or args.secondary_delay_ms < 0:
        parser.error('repeats must be positive and delay nonnegative')
    valid = asyncio.run(run(args.base_url, args.scenario, args.output, args.repeats, args.worker_delay_ms, args.secondary_delay_ms))
    raise SystemExit(0 if valid else 1)


if __name__ == '__main__':
    main()
