"""Minimal interactive SQL shell over the gold tables: `make sql`."""

from dashboard.gold_store import connect


def main() -> None:
    con, tables = connect()
    print(f"Gold tables: {', '.join(tables) or '(none yet)'}  —  end with ';', quit with \\q")
    buffer = ""
    while True:
        try:
            line = input("gold> " if not buffer else "  ... ")
        except EOFError:
            break
        if line.strip() == "\\q":
            break
        buffer += line + "\n"
        if buffer.strip().endswith(";"):
            try:
                con.sql(buffer).show(max_rows=50)
            except Exception as exc:
                print(f"Error: {exc}")
            buffer = ""


if __name__ == "__main__":
    main()
