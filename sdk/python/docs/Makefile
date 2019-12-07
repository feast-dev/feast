# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    	=
SPHINXBUILD   	= sphinx-build
SPHINXPROJ    	= Feast
SOURCEDIR     	= source
BUILDDIR      	= build
PROJECT_ROOT 	:= $(shell git rev-parse --show-toplevel)

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile source

clean:
	rm -rf $(PROJECT_ROOT)/sdk/python/feast/docs/source
	rm -rf $(PROJECT_ROOT)/sdk/python/feast/docs/build

build-api-source:
	sphinx-apidoc -f -o source $(PROJECT_ROOT)/sdk/python/feast

build-html-docs: build-api-source
	sphinx-build -b html source build

html: build-html-docs
	mkdir -p $(PROJECT_ROOT)/dist/python
	mv $(PROJECT_ROOT)/sdk/python/docs/build/html/* $(PROJECT_ROOT)/dist/python/

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile source
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)