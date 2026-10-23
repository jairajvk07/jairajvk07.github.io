# Validation status

Python syntax checks and two synthetic-fixture contract tests passed in the authoring environment. Three Spark integration tests are provided but were not executed here because Java was unavailable. They cover CDC replay and stale inserts after deletion, streaming duplicate removal and invalid records, and late events after watermark advancement.

The GitHub workflow runs the lightweight checks on push and pull requests. Start it manually with the integration option enabled to run actual Spark tests with Java 17 and Python 3.11. Network access is required for Maven artifacts. Kafka broker/producer behavior and production deployment have not been validated here. Expected demo counts are fixture expectations rather than measured results.
