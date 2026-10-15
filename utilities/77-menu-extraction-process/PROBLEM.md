# Problem: website menus are not in the warehouse

Restaurant websites are the source of menu pages, and most of them are a poor fit for a plain HTTP client running inside Composer.

What we actually have upstream is a business listing: name, country, website, sometimes a menu URL, sometimes nothing. What downstream scoring and menu-gap work needs is a stable menu page per establishment, then the text of that page. Those are two different jobs, and mixing them in one task is how this pipeline got stuck the first time.

## What goes wrong with HTTP-only fetch

- A lot of menu links only appear after JavaScript. `requests.get` returns a shell page and discovery finds zero menu URLs.
- Bot walls answer 403 / 429 / 503. Retrying the same GET from the worker IP does not help.
- Chromium inside a Composer worker is the wrong place for that. Image size, `/dev/shm`, and task timeouts fight you, and a hung browser pins a worker slot.
- Once a URL is found, pulling the HTML, turning it into markdown, and asking a model to read it are a second pipeline. Doing that in the same DAG as URL discovery made both sides hard to rerun.

## What "done" means

1. A ledger row per establishment with the menu URL we will trust, and a flag for whether Cloud Run had to render the page.
2. A page-level menu row (title, cleaned text, section JSON). Not one row per dish. Dish itemization is a later process.
3. Failed URLs stay on a control table and come back next run. A bad page does not block the country.

Patterns 18 and 75 are the detailed task graphs for the two DAGs. This folder is the contract that sits across them: Cloud Run render, the Airflow handoff, and the BigQuery tables.
