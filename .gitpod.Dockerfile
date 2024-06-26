FROM gitpod/workspace-base
RUN sudo apt-get update && sudo apt-get install -y python3-dev python3-setuptools python3-pip python-is-python3 && sudo rm -rf /var/lib/apt/lists/*
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
RUN curl -fsSL https://pixi.sh/install.sh | bash
ENV PATH=$PATH:/home/gitpod/.cargo/bin
RUN curl -s "https://get.sdkman.io" | bash
SHELL ["/bin/bash", "-c"]
RUN source "/home/gitpod/.sdkman/bin/sdkman-init.sh" && sdk install java 14.0.2-zulu