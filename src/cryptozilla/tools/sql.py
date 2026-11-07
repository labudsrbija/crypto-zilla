"""Run SQL against the lakehouse: python -m cryptozilla.tools.sql "SELECT ..." """

import sys

from cryptozilla.spark import make_spark_session


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit('usage: python -m cryptozilla.tools.sql "<sql>" [max_rows]')
    rows = int(sys.argv[2]) if len(sys.argv) > 2 else 50
    spark = make_spark_session("cz-sql", master="local[2]")
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(sys.argv[1]).show(rows, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()