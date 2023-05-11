libbech32.so: bech32.c
	gcc -shared -g -fPIC -O2 -o $@ -Ibech32/ref/c $<
