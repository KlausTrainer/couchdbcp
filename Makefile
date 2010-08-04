all: ebin/ doc/
	(cd src;$(MAKE))

clean:
	rm -rf log/sasl/*
	rm -f erl_crash.dump
	(cd src;$(MAKE) clean)

ebin:
	@mkdir -p ebin

doc:
	@mkdir -p doc
