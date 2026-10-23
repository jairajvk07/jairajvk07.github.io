# Validation status

Verified October 23, 2026.

## Automated checks

All five tests passed in GitHub Actions with Python 3.11, Java 17, Spark 3.5.3 and Delta 3.2.1. The full run completed the test suite in 81.436 seconds.

- Synthetic CDC fixture contract.
- Synthetic transaction fixture contract.
- Delta CDC replay and protection against stale inserts after deletion.
- Dropping late events after the event-time watermark advances.
- Streaming transaction deduplication, invalid amount labeling, and raw audit counts.

[Full integration run](https://github.com/jairajvk07/jairajvk07.github.io/actions/runs/40001382328)

The workflow runs syntax and fixture checks on pushes and pull requests. Run it manually with the integration option enabled to repeat the Spark tests. The Kafka broker and producer were not exercised in this run; streaming integration uses the file source. Snowflake loading and production-scale throughput remain outside the validation scope.

## Website

[Live portfolio](https://jairajvk07.github.io/)

GitHub Pages deployment succeeded. The public website was opened and inspected at desktop and 390-pixel phone viewport widths. Mobile document width stayed within the viewport. Project navigation and expandable local setup instructions were checked. Both project source directories are committed, and a downloadable synthetic demo bundle is available.

The projects are original educational demonstrations. No employer systems, real transactions, or private datasets were used.
