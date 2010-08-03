include ../support/include.mk

all: $(EBIN_FILES)

debug:
	$(MAKE) DEBUG=-DDEBUG

clean:
	rm -rf ../log/sasl/*
	rm -rf erl_crash.dump
	rm -rf $(EBIN_FILES)
