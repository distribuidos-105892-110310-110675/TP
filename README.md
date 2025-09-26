# TP - Coffee Shop Analysis

## Materia: Sistemas Distribuidos 1 (Roca)

## Grupo 9

### Integrantes

| Nombre                                                          | Padrón |
| --------------------------------------------------------------- | ------ |
| [Ascencio Felipe Santino](https://github.com/FelipeAscencio)    | 110675 |
| [Gamberale Luciano Martín](https://github.com/lucianogamberale) | 105892 |
| [Zielonka Axel](https://github.com/axel-zielonka)               | 110310 |

### Corrector

- Franco Papa.

## Descripción

En este repositorio se encuentra el material relacionado al TP del sistema distribuido "Coffee Shop Analysis" del segundo cuatrimestre del año 2025 en la materia "Sistemas Distribuidos 1 (Roca)".

## Enunciado

Para poder acceder los enunciados del trabajo práctico, haga click [aquí](./docs/).

## Informe de Diseño

El informe técnico detalla las decisiones de diseño y la implementación de cada ejercicio, incluyendo el protocolo de comunicación y los mecanismos de concurrencia utilizados. Además, se indica cómo debe ser ejecutado cada ejericio. Para acceder al informe, haga click [aquí](./docs/Informe-G9-Diseño.pdf).

---

---

<br>
<p align="center">
  <img src="https://huergo.edu.ar/images/convenios/fiuba.jpg" width="60%" style="background-color:white"/>
<font size="+1">
<br>
<br>
2c 2025
</font>
</p>

---

---

## Levantar los containers de Docker de cada Controlador

### Construir la imagen

```bash

docker build -t sd1-cleaner:dev .

```

### Correr por defecto ('menu_cleaner', 'q1_output_builder' y 'count_purchases')

```bash

docker run --name <nombre_del_controlador> --rm -e CLEANER_SLEEP_SECS=1 sd1-cleaner:dev

```

### Correr en primer plano (Para probar SIGINT)

```bash

docker run --name <nombre_del_controlador> --rm \
  -e CLEANER_SLEEP_SECS=1 \
  sd1-cleaner:dev \
  <nombre_del_controlador>.py

```

### Correr en segundo plano (Para probar SIGTERM)

```bash

docker run -d --name <nombre-del-controlador> \
  -e CLEANER_SLEEP_SECS=1 \
  sd1-cleaner:dev \
  <nombre-del-controlador>.py

docker logs -f <nombre-del-controlador>

docker stop <nombre-del-controlador>

```

### Bajar todos los containers si quedaron corriendo

```bash

docker stop $(docker ps -aq)

docker rm $(docker ps -aq)

docker ps -a # Verificar que se bajaron todos.

```
