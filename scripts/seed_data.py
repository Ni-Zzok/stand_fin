"""Seed through the API so a running worker cannot race a direct database reset."""
import argparse
import httpx


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--base-url', default='http://127.0.0.1:8000')
    args = parser.parse_args()
    with httpx.Client(base_url=args.base_url, timeout=120) as client:
        client.post('/experiments/reset').raise_for_status()
        response = client.post('/experiments/seed')
        response.raise_for_status()
        print(response.json())


if __name__ == '__main__':
    main()
