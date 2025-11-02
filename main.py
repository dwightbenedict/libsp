import asyncio
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from chaoxing.core.config import config
import scraper


def scraper_process(institution_hostname: str, db_url: str) -> None:
    asyncio.run(scraper.scrape_institution(institution_hostname, db_url))


def read_institution_hostnames(file_path: Path) -> list[str]:
    with file_path.open("r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def main() -> None:
    hostnames_file = Path("data/institution_hostnames.txt")
    institution_hostnames = read_institution_hostnames(hostnames_file)

    with ProcessPoolExecutor(max_workers=config.max_workers) as executor:
        futures = {
            executor.submit(scraper_process, hostname, config.db_url): hostname
            for hostname in institution_hostnames
        }

        for future in as_completed(futures):
            hostname = futures[future]
            try:
                future.result()
                print(f"✅ Completed: {hostname}")
            except Exception as e:
                print(f"❌ Failed: {hostname} — {e}")


if __name__ == "__main__":
    mp.set_start_method("spawn")
    main()
