.PHONY: run cli test benchmark clean help

PYTHON ?= python3

help:
	@echo "Available targets:"
	@echo "  make run         Start pyredis server on 127.0.0.1:6379"
	@echo "  make cli         Start interactive pyredis-cli REPL"
	@echo "  make test        Run all unit and integration tests"
	@echo "  make benchmark   Run high-throughput latency benchmark"
	@echo "  make clean       Remove temporary files, AOF logs, RDB dumps"

run:
	$(PYTHON) -m pyredis

cli:
	$(PYTHON) -m pyredis.cli

test:
	$(PYTHON) -m unittest discover tests

benchmark:
	$(PYTHON) benchmarks/benchmark.py -p 6379 -n 10000 -c 10 -P 16

clean:
	rm -rf *.aof* *.rdb* __pycache__ */__pycache__ tests/__pycache__ .pytest_cache
