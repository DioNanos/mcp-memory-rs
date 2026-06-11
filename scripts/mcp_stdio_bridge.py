#!/usr/bin/env python3

import subprocess
import sys
import threading


def read_frame(stream):
    content_length = None
    saw_header = False

    while True:
        line = stream.readline()
        if line == b"":
            if saw_header:
                raise EOFError("unexpected EOF while reading frame headers")
            return None
        saw_header = True
        line = line.rstrip(b"\r\n")
        if not line:
            break
        name, sep, value = line.partition(b":")
        if sep and name.lower() == b"content-length":
            content_length = int(value.strip())

    if content_length is None:
        raise ValueError("missing Content-Length")

    body = stream.read(content_length)
    if len(body) != content_length:
        raise EOFError("unexpected EOF while reading frame body")
    return body


def write_frame(stream, body):
    header = f"Content-Length: {len(body)}\r\n\r\n".encode()
    stream.write(header)
    stream.write(body)
    stream.flush()


def forward_child_stdout(child_stdout):
    try:
        for line in iter(child_stdout.readline, b""):
            line = line.rstrip(b"\r\n")
            if not line:
                continue
            write_frame(sys.stdout.buffer, line)
    finally:
        child_stdout.close()


def forward_child_stderr(child_stderr):
    try:
        for chunk in iter(lambda: child_stderr.read(4096), b""):
            sys.stderr.buffer.write(chunk)
            sys.stderr.buffer.flush()
    finally:
        child_stderr.close()


def main():
    if len(sys.argv) < 2:
        print("Usage: mcp_stdio_bridge.py <child-command> [args...]", file=sys.stderr)
        return 2

    child = subprocess.Popen(
        sys.argv[1:],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )

    stdout_thread = threading.Thread(
        target=forward_child_stdout,
        args=(child.stdout,),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=forward_child_stderr,
        args=(child.stderr,),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    try:
        while True:
            frame = read_frame(sys.stdin.buffer)
            if frame is None:
                break
            child.stdin.write(frame + b"\n")
            child.stdin.flush()
    except (EOFError, BrokenPipeError):
        pass
    finally:
        try:
            child.stdin.close()
        except Exception:
            pass

        rc = child.wait()
        stdout_thread.join(timeout=1)
        stderr_thread.join(timeout=1)
        return rc


if __name__ == "__main__":
    raise SystemExit(main())
