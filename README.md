# Jairaj Jagarapu Portfolio

A responsive data engineering portfolio with a dark theme, experience, skills, education, and detailed project case studies.

**[View the live portfolio](https://jairajvk07.github.io/)**

## Project repositories

The demonstration projects have dedicated repositories so their code, setup, and test results can be reviewed independently.

| Project | Repository |
| --- | --- |
| Config Driven Lakehouse | [PySpark and Delta Lake CDC pipeline](https://github.com/jairajvk07/config-driven-lakehouse) |
| Streaming Transaction Quality Monitor | [Kafka and Spark event-time quality pipeline](https://github.com/jairajvk07/streaming-transaction-quality-monitor) |

Both use synthetic data. They are portfolio demonstrations, separate from employer systems and employment outcomes.

## Website structure

- `index.html` — profile, experience, skills, education, contact, and project case studies
- `style.css` — layout, typography, and responsive breakpoints
- `dark.css` — dark theme and visual treatments
- `portfolio-demos.zip` — downloadable combined demonstration source bundle
- `projects/` and `tests/` — original combined project layout retained for reference
- `PROVENANCE.md` — resume source details
- `VALIDATION.md` — checks and verification scope

## Local preview

```sh
python -m http.server 8080
```

Open http://localhost:8080. The site needs no build step, API key, or application backend.

## Deployment

GitHub Pages serves the `main` branch at `/ (root)`. Updates to the website files are deployed automatically. Keep local paths relative so styles and downloads work on the hosted site.

For current standalone project setup, runtime integration tests, and architecture documentation, use the two project repositories linked above.
