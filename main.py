from __future__ import annotations

from pipeline.web_app import run_server


def main() -> None:
    run_server(open_browser=True)


if __name__ == "__main__":
    main()
