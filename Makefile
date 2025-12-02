PYTHON = python3
MAIN = main.py

all: run

run:
	$(PYTHON) $(MAIN)

clean:
	rm -rf __pycache__ */__pycache__ .pytest_cache

fclean: clean
	rm -f *.log
	clear

re: fclean all
