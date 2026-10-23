import json
import os
import unittest
from pathlib import Path


class FixtureContracts(unittest.TestCase):
    def test_cdc_fixture_exercises_updates_deletes_and_rejections(self):
        rows = [json.loads(x) for x in Path('projects/lakehouse/data/cdc.jsonl').read_text().splitlines()]
        self.assertEqual({r['op'] for r in rows}, {'I','U','D'})
        self.assertEqual(sum(r['customer_id'] is None for r in rows), 1)

    def test_transaction_fixture_has_duplicates_and_late_event(self):
        rows = [json.loads(x) for x in Path('projects/streaming/data/transactions.jsonl').read_text().splitlines()]
        self.assertEqual(rows[0], rows[1])
        self.assertLess(rows[4]['event_time'], rows[3]['event_time'])
        self.assertLess(rows[2]['amount'], 0)


@unittest.skipUnless(os.environ.get('RUN_SPARK_TESTS') == '1', 'Set RUN_SPARK_TESTS=1 with Java and Spark installed')
class SparkIntegration(unittest.TestCase):
    def test_late_event_after_watermark_advances(self):
        import tempfile
        from pyspark.sql import SparkSession
        from projects.streaming.stream import SCHEMA
        with tempfile.TemporaryDirectory() as tmp:
            spark = SparkSession.builder.master('local[2]').getOrCreate()
            source = Path(tmp, 'input'); source.mkdir()
            events = spark.readStream.schema(SCHEMA).json(str(source))
            query = (events.withWatermark('event_time','10 minutes').dropDuplicatesWithinWatermark(['transaction_id'])
                .writeStream.format('memory').queryName('late_test').outputMode('append').start())
            try:
                def emit(index, key, time):
                    Path(source, f'{index}.json').write_text(json.dumps({'transaction_id':key,'event_time':time,'amount':1,'currency':'USD'}))
                    query.processAllAvailable()
                emit(1, 'first', '2026-01-01T12:00:00Z')
                emit(2, 'advance', '2026-01-01T12:30:00Z')
                emit(3, 'late', '2026-01-01T11:00:00Z')
                self.assertEqual(spark.sql("select * from late_test where transaction_id='late'").count(), 0)
                self.assertGreater(sum(op.get('numRowsDroppedByWatermark',0) for p in query.recentProgress for op in p.get('stateOperators',[])), 0)
            finally:
                query.stop(); spark.stop()

    def test_delta_cdc_replay_and_stale_delete(self):
        import tempfile
        from projects.lakehouse.pipeline import run
        with tempfile.TemporaryDirectory() as tmp:
            config = json.loads(Path('projects/lakehouse/config.json').read_text())
            config['root'] = f'{tmp}/lake'
            first = run(config)
            self.assertEqual(first['active_customers'], 2)
            self.assertEqual(first['quarantined_rows'], 1)
            self.assertEqual(run(config), first)
            stale = Path(tmp, 'stale.jsonl')
            stale.write_text(json.dumps({'customer_id':'C02','sequence':1,'op':'I','name':'Morgan','region':'West'}))
            config['source'] = str(stale)
            self.assertEqual(run(config)['active_customers'], 2)

    def test_streaming_dedup_quality_and_audit(self):
        import tempfile
        from pyspark.sql import SparkSession
        from projects.streaming.stream import run
        import argparse
        with tempfile.TemporaryDirectory() as tmp:
            run(argparse.Namespace(source='files', input='projects/streaming/data', output=tmp, seconds=25))
            spark = SparkSession.builder.master('local[2]').getOrCreate()
            try:
                rows = spark.read.parquet(f'{tmp}/batches/*/rows')
                self.assertEqual(rows.filter("transaction_id = 'T01'").count(), 1)
                self.assertEqual(rows.filter("transaction_id = 'T02' AND quality_status = 'invalid'").count(), 1)
                self.assertEqual(spark.read.parquet(f'{tmp}/audit').count(), 6)
            finally:
                spark.stop()


if __name__ == '__main__':
    unittest.main()
