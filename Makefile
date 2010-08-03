all:
	(cd src;$(MAKE))

clean:
	rm -rf log/sasl/*
	rm -f erl_crash.dump
	(cd src;$(MAKE) clean)
