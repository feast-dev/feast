# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    	=
SPHINXBUILD   	= python -m sphinx
SPHINXPROJ    	= Feast
SOURCEDIR     	= source
BUILDDIR      	= build
PROJECT_ROOT 	:= $(shell git rev-parse --show-toplevel)

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile source

clean-html:
	rm -rf $(PROJECT_ROOT)/sdk/python/docs/html

build-api-source:
	sphinx-apidoc -f -o source $(PROJECT_ROOT)/sdk/python/feast
	cp conf.py index.rst source/

html: clean-html build-api-source
	python -m sphinx -b html source html

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile source
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)