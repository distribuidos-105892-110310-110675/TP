# syntax=docker/dockerfile:1
FROM python:3.11-slim

# Ajustes de Python: sin .pyc y salida sin buffer
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CLEANER_SLEEP_SECS=2

# Crear usuario no-root
RUN useradd -m appuser
WORKDIR /app

# Copiamos TODO el árbol de cleaners (subcarpetas incluidas)
# IMPORTANTE: construir desde src/controllers/cleaners/ para que el contexto sea correcto.
COPY . /app/

# Permisos y usuario
RUN chown -R appuser:appuser /app
USER appuser

# Señal por defecto al apagar el contenedor
STOPSIGNAL SIGTERM

# Modo genérico: el entrypoint es "python -u"
# y el archivo a ejecutar se pasa como argumento al `docker run`.
ENTRYPOINT ["python", "-u"]

# Opción por defecto (se puede sobreescribir al correr):
# si no pasás nada, intentará ejecutar menu_cleaner.py
CMD ["menu_cleaner.py"]
