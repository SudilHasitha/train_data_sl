import asyncio, re, pandas as pd
from pathlib import Path
from playwright.async_api import async_playwright

SEARCH_URL = "https://eservices.railway.gov.lk/schedule/searchTrain.action?lang=en"

# Simple helpers to normalize text
def norm(s): return " ".join((s or "").split())

async def scrape_train_stops_for_route(page, start_id: int, end_id: int):
    await page.goto(SEARCH_URL, wait_until="domcontentloaded")

    # Select start/end by station ID (option values are numeric strings)
    await page.locator("#startStation").select_option(str(start_id))
    await page.locator("#endStation").select_option(str(end_id))

    # (Optional) choose time windows if you want:
    # await page.locator('select[name="searchCriteria.startTime"]').select_option("00:00:01")
    # await page.locator('select[name="searchCriteria.endTime"]').select_option("23:59:59")

    # Press Search
    # First button with class 'es-button' has onclick='formSubmit()'
    await page.locator('button.es-button:has-text("Search")').click()
    await page.wait_for_selector('text=Direct Trains', timeout=30000)

    # Each “train card” is usually a block that contains “Train No: ####”.
    cards = page.locator("xpath=//*[contains(normalize-space(text()), 'Train No')]")
    n = await cards.count()

    trains = []
    for i in range(n):
        node = cards.nth(i)
        text = await node.inner_text()
        m = re.search(r"Train No:\s*([0-9]+)", text, flags=re.I)
        if not m:  # try reading a bigger container (parent row)
            container = node.locator("xpath=ancestor::*[self::div or self::section][1]")
            text = await container.inner_text()
            m = re.search(r"Train No:\s*([0-9]+)", text, flags=re.I)
        if not m:
            continue

        train_no = m.group(1)

        # Try to expand / open per-station schedule if there is a link or button in the same card
        # Heuristics: look for a clickable that contains 'Details', 'Schedule', or the train number itself.
        detail = (
            node.locator("xpath=.//a[contains(., 'Detail') or contains(., 'Schedule') or contains(., 'Train')]")
            .first
        )
        if await detail.count() == 0:
            # Sometimes the whole row is clickable—try clicking the nearest link in the parent block
            detail = node.locator("xpath=ancestor::*[self::div or self::section][1]//a").first

        stops = []
        try:
            if await detail.count() > 0:
                # open in same tab
                await detail.click()
                # Wait for a table with headers Arrival / Departure / Station (like in your screenshot)
                await page.wait_for_selector("table >> text=/Arrival Time/i", timeout=10000)
                table = page.locator("table").first
                rows = table.locator("tr")
                for r in range(await rows.count()):
                    tds = rows.nth(r).locator("td")
                    if await tds.count() >= 3:
                        arr = norm(await tds.nth(0).inner_text())
                        dep = norm(await tds.nth(1).inner_text())
                        stn = norm(await tds.nth(2).inner_text())
                        # skip header-ish rows
                        if arr.lower().startswith("arrival") or stn.lower() == "station":
                            continue
                        stops.append({"train_no": train_no, "arrival": arr, "departure": dep, "station": stn})

                # go back to results if we navigated away
                # (If it opened in a modal, there may be nothing to go back to.)
                if page.url.endswith(".action") and "searchTrain" not in page.url:
                    await page.go_back()
                    await page.wait_for_selector('text=Direct Trains', timeout=15000)

        except Exception:
            # Fallback: if there’s no details table, we still record the summary
            pass

        trains.append({"train_no": train_no, "stops": stops})

    return trains

async def main():
    # Choose routes to traverse (add as many as you like).
    # Example: Colombo Fort(61) -> Matara(187); Kandy(115) -> Hatton(96)
    routes = [(61, 187), (115, 96)]

    out = Path("train_stops")
    out.mkdir(exist_ok=True)
    all_rows = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = await browser.new_page()
        for sid, eid in routes:
            data = await scrape_train_stops_for_route(page, sid, eid)
            for entry in data:
                tno = entry["train_no"]
                stops = entry["stops"]
                if stops:
                    df = pd.DataFrame(stops)
                    csv_path = out / f"train_{tno}.csv"
                    df.to_csv(csv_path, index=False)
                    all_rows.extend(stops)
        await browser.close()

    if all_rows:
        dfall = pd.DataFrame(all_rows)
        try:
            dfall.to_excel("train_stops.xlsx", index=False)
        except Exception:
            pass
        print(f"Saved per-train CSVs in ./train_stops and combined train_stops.xlsx")

if __name__ == "__main__":
    asyncio.run(main())
