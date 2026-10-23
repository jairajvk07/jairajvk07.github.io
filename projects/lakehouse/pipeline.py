"""Synthetic CDC demo. Run from repository root with python -m projects.lakehouse.pipeline."""
import argparse
import json
from pathlib import Path
from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession, functions as F, Window


def run(config):
    key, seq, root = config['key'], config['sequence'], config['root']
    builder = (SparkSession.builder.master('local[2]').appName('portfolio-lakehouse')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog'))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    try:
        raw = spark.read.schema(f'{key} string, {seq} long, op string, name string, region string').json(config['source'])
        # Bronze is a replaceable demo run snapshot.
        raw.write.format('delta').mode('overwrite').save(f'{root}/bronze')
        valid = F.col(key).isNotNull() & F.col(seq).isNotNull() & F.col('op').isin(config['allowed_ops'])
        raw.filter(~valid).write.format('delta').mode('overwrite').save(f'{root}/quarantine')
        good = raw.filter(valid)
        # Reject conflicting events rather than choosing nondeterministically.
        conflicts = good.groupBy(key, seq).agg(F.countDistinct(F.to_json(F.struct('op','name','region'))).alias('versions'))
        if conflicts.filter('versions > 1').count():
            raise ValueError('Conflicting payloads for the same key and sequence')
        latest = good.withColumn('_rn', F.row_number().over(Window.partitionBy(key).orderBy(F.col(seq).desc()))).filter('_rn = 1').drop('_rn')
        path = f'{root}/silver'
        if not DeltaTable.isDeltaTable(spark, path):
            latest.limit(0).write.format('delta').save(path)
        # Tombstones retain sequence so stale replays cannot resurrect deletions.
        (DeltaTable.forPath(spark, path).alias('t').merge(latest.alias('s'), f't.{key} = s.{key}')
            .whenMatchedUpdateAll(condition=f's.{seq} > t.{seq}').whenNotMatchedInsertAll().execute())
        silver = spark.read.format('delta').load(path)
        active = silver.filter("op != 'D'")
        active.groupBy('region').count().write.format('delta').mode('overwrite').save(f'{root}/gold')
        assert silver.groupBy(key).count().filter('count > 1').count() == 0
        assert active.filter(F.col(key).isNull()).count() == 0
        report = {'input_rows':raw.count(), 'quarantined_rows':raw.filter(~valid).count(), 'active_customers':active.count(), 'unique_keys':True}
        Path(root).mkdir(parents=True, exist_ok=True)
        Path(root, 'quality.json').write_text(json.dumps(report, indent=2))
        return report
    finally:
        spark.stop()


if __name__ == '__main__':
    p = argparse.ArgumentParser(); p.add_argument('--config', default='projects/lakehouse/config.json')
    print(json.dumps(run(json.loads(Path(p.parse_args().config).read_text())), indent=2))
