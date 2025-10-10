import csv
from pathlib import Path

def add_columns(source_path: Path, new_path: Path) -> None:
    new_path.parent.mkdir(parents=True, exist_ok=True)
    with source_path.open("r", newline='', encoding="utf-8") as source_file, \
        new_path.open("w", newline= '', encoding="utf-8") as new_file:
        reader = csv.DictReader(source_file)
        existing_columns = list(reader.fieldnames or [])
        for newcol in ("ASM", "RPM"):
            if newcol not in existing_columns:
                existing_columns.append(newcol)
        writer = csv.DictWriter(new_file, fieldnames=existing_columns)
        writer.writeheader()

        for row in reader:
            # robust parse: handles "0.00", "1,234", blanks
            def num(s):
                if s is None: return 0.0
                s = s.strip().replace(",", "")
                if s == "": return 0.0
                try:
                    return float(s)
                except ValueError:
                    return 0.0

            seats = num(row.get("SEATS"))
            pax   = num(row.get("PASSENGERS"))
            dist  = num(row.get("DISTANCE"))

            asm = seats * dist
            rpm = pax * dist
            row["ASM"] = str(int(round(asm)))
            row["RPM"] = str(int(round(rpm)))

            writer.writerow(row)