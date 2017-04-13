sources := $(patsubst %.k8s.js,%.k8s.made.json,$(wildcard *.k8s.js))

all: $(sources)

%.k8s.made.json: %.k8s.js
	node $< > $@

clean:
	rm -rf $(sources)
