all: bfsvtab.so

bfsvtab.so: bfsvtab.c
	gcc -g -Wall -Wextra -pedantic -fPIC -shared -I./sqlite bfsvtab.c -o bfsvtab.so

test: bfsvtab.so
	./test.sh

benchmark: bfsvtab.so
	./benchmark.sh

clean:
	- rm bfsvtab.so

.PHONY=all test clean
