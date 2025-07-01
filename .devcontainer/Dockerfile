FROM mcr.microsoft.com/vscode/devcontainers/python:3.12

USER vscode
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
RUN curl -fsSL https://pixi.sh/install.sh | bash
ENV PATH=$PATH:/home/vscode/.cargo/bin
ENV PYTHON=3.12
RUN uv venv ~/.local
ENV VIRTUAL_ENV=~/.local
ENV PATH=$VIRTUAL_ENV/bin:$PATH
USER root
    
    