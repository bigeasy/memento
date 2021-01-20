all: test/readme.t.js README.md

test/readme.t.js: README.in.md
	moxie --mode code $< > $@
README.md: README.in.md
	moxie --mode text $< > $@
