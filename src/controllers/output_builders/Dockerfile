# syntax=docker/dockerfile:1
FROM python:3.11-slim

# Evita escribir .pyc y asegura salida sin buffer
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CLEANER_SLEEP_SECS=2

# Crea usuario no-root
RUN useradd -m appuser
WORKDIR /app

# Solo copiamos lo necesario para este nodo
# (este Dockerfile debe vivir en la misma carpeta que menu_cleaner.py)
COPY menu_cleaner.py /app/menu_cleaner.py

# Cambiamos permisos y usuario
RUN chown -R appuser:appuser /app
USER appuser

# Señal por defecto al apagar el contenedor
STOPSIGNAL SIGTERM

# IMPORTANTE: usar forma exec para que Docker reenvíe señales al PID 1
ENTRYPOINT ["python", "-u", "menu_cleaner.py"]