"""
Interactive Terminal REPL (CLI) for pyredis.
Features:
- ANSI colored output
- Quoted string argument parsing (e.g. SET "my key" "my value with spaces")
- Tab completion and history support via readline
- Supports connecting to pyredis or standard Redis servers
"""
import argparse
import readline
import shlex
import sys
from typing import Any

from pyredis.client import Client, RedisError

# ANSI color codes
RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
GRAY = "\033[90m"


def format_response(resp: Any, indent: int = 0) -> str:
    """Format RESP return values for terminal display."""
    pad = "  " * indent

    if resp is None:
        return f"{GRAY}(nil){RESET}"
    elif isinstance(resp, str):
        return f'{GREEN}"{resp}"{RESET}'
    elif isinstance(resp, bytes):
        try:
            decoded = resp.decode("utf-8")
            return f'{GREEN}"{decoded}"{RESET}'
        except UnicodeDecodeError:
            return f"{YELLOW}{resp!r}{RESET}"
    elif isinstance(resp, int):
        return f"{CYAN}(integer) {resp}{RESET}"
    elif isinstance(resp, float):
        return f'{GREEN}"{resp:g}"{RESET}'
    elif isinstance(resp, list):
        if not resp:
            return f"{GRAY}(empty array){RESET}"
        lines = []
        for i, item in enumerate(resp, 1):
            formatted_item = format_response(item, indent + 1)
            lines.append(f"{pad}{i}) {formatted_item}")
        return "\n".join(lines)
    elif isinstance(resp, dict):
        if not resp:
            return f"{GRAY}(empty hash){RESET}"
        lines = []
        for i, (k, v) in enumerate(resp.items(), 1):
            fk = format_response(k)
            fv = format_response(v)
            lines.append(f"{pad}{i}) {fk} -> {fv}")
        return "\n".join(lines)
    return str(resp)


def main():
    parser = argparse.ArgumentParser(description="pyredis-cli - Interactive Redis Terminal Client")
    parser.add_argument("-H", "--host", default="127.0.0.1", help="Server hostname (default: 127.0.0.1)")
    parser.add_argument("-p", "--port", type=int, default=6379, help="Server port (default: 6379)")
    args = parser.parse_args()

    print(f"{BOLD}pyredis-cli 1.0.0{RESET}")
    print(f"Connecting to {args.host}:{args.port}...")

    try:
        client = Client(host=args.host, port=args.port)
        print(f"{GREEN}Connected! Type 'help' for commands, or 'exit' to quit.{RESET}\n")
    except Exception as e:
        print(f"{RED}Could not connect to Redis at {args.host}:{args.port}: {e}{RESET}")
        sys.exit(1)

    while True:
        try:
            prompt = f"{args.host}:{args.port}> "
            line = input(prompt).strip()
            if not line:
                continue

            if line.lower() in ("exit", "quit"):
                print("Goodbye!")
                break

            # Parse quoted strings
            try:
                tokens = shlex.split(line)
            except ValueError as e:
                print(f"{RED}(error) Parse error: {e}{RESET}")
                continue

            if not tokens:
                continue

            # Execute command
            try:
                resp = client.execute_command(*tokens)
                print(format_response(resp))
            except RedisError as e:
                print(f"{RED}(error) {e}{RESET}")
            except ConnectionError:
                print(f"{RED}Server closed the connection. Exiting.{RESET}")
                break

        except (KeyboardInterrupt, EOFError):
            print("\nExiting...")
            break

    client.close()


if __name__ == "__main__":
    main()
