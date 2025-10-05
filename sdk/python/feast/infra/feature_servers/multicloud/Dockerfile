FROM registry.access.redhat.com/ubi9/python-311:1

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# modify permissions to support running with a random uid
RUN chmod g+w $(python -c "import feast.ui as ui; print(ui.__path__)" | tr -d "[']")/build/projects-list.json
