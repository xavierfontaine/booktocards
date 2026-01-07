.PHONY: format lint check_type

format:
	black booktocards/ tests/ scripts/
	isort booktocards/ tests/ scripts/ --profile=black
lint:
	flake8 --ignore=E203,E501,E701,E721,W503,W605
check_type:
	mypy booktocards/ tests/ scripts/
