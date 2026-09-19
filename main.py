import sys

def main():
    if "--civic" in sys.argv:
        sys.argv.remove("--civic")
        from ui.civic_window import main as civic_main
        return civic_main()
    from ui.web_window import run_app
    return run_app(sys.argv)


if __name__ == "__main__":
    raise SystemExit(main())
