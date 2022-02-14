PYTHON_VERSION ?= 3.7.10
PATH_TO_FILE ?= ~/.pyenv/versions/${PYTHON_VERSION}

all: create-plugin-binary

create-plugin-binary:
# If package is installed, remove and reinstall with --enable-framework
ifeq ("$(wildcard "${PATH_TO_FILE}")","")
	echo "${PATH_TO_FILE} exists"
	pyenv uninstall ${PYTHON_VERSION}
endif
	env PYTHON_CONFIGURE_OPTS="--enable-framework" pyenv install ${PYTHON_VERSION}
	pip install pyinstaller
	pip install .
	sudo pyinstaller plugin.py --onefile