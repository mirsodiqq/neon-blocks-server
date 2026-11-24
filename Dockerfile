# NEON BLOCKS server - no dependencies, tiny image.
FROM python:3.12-slim
WORKDIR /app
COPY server.py ws.py game_rules.py ./
ENV PORT=8080
EXPOSE 8080
USER nobody
CMD ["python3", "server.py"]
