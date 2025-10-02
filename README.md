<br>
<p align="center">
  <img src="https://huergo.edu.ar/images/convenios/fiuba.jpg" width="100%" style="background-color:white"/>
</p>

# ☕ Coffee Shop Analysis

## 📚 Materia: Sistemas Distribuidos 1 (Roca)

## 👥 Grupo 9

### Integrantes

| Nombre                                                          | Padrón |
| --------------------------------------------------------------- | ------ |
| [Ascencio Felipe Santino](https://github.com/FelipeAscencio)    | 110675 |
| [Gamberale Luciano Martín](https://github.com/lucianogamberale) | 105892 |
| [Zielonka Axel](https://github.com/axel-zielonka)               | 110310 |

### Corrector

- [Franco Papa](https://github.com/F-Papa)

## 📖 Descripción

Este repositorio contiene el material del TP del sistema distribuido "Coffee Shop Analysis", correspondiente al segundo cuatrimestre del año 2025 en la materia Sistemas Distribuidos 1 (Roca).

## 📂 Enunciado

Para acceder al enunciado del TP, haga click 👉 [aquí](./docs/).

## 🛠️ Informe de Diseño

El informe técnico incluye:
- Decisiones de diseño.
- Implementación de cada ejercicio.
- Protocolo de comunicación.
- Mecanismos de concurrencia utilizados.
- Instrucciones de ejecución.

[📑 Acceso al informe](./docs/Informe-G9-Diseño.pdf).

## 🚀 Ejecución del Sistema con Docker Compose

En este TP se utiliza Docker Compose para levantar todos los componentes:
- El cliente que realiza las queries.
- El servidor que las recibe.
- Todos los nodos del sistema distribuido.
- El middleware.

### ▶️ Levantar todo el sistema

```bash

make docker-compose-up

```

✅ Con este comando se ponen en marcha todos los servicios del sistema distribuido (cliente, server, nodos y middleware).

### ⏹️ Apagar todo el sistema

```bash

make docker-compose-down

```

❌ Detiene y elimina todos los contenedores que levantó el sistema.


### 📜 Ver los logs del sistema

```bash

make docker-compose-logs

```

👀 Muestra en consola todos los logs de cada componente.

### 🔎 Filtrar logs de un contenedor específico

```bash

make docker-compose-logs | grep '<nombre_del_contenedor>'

```

👉 Esto mostrará solo los logs de los filters, lo cual es práctico para debuggear sin ruido de otros componentes.

#### Ejemplo:

```bash

make docker-compose-logs | grep 'filter'

```

## 📡 Monitorear RabbitMQ

Dado que el sistema utiliza RabbitMQ para la comunicación, podés seguir en tiempo real el estado de las colas, los mensajes que viajan y cómo se encadenan los procesos.

1. Primero, asegurate de haber levantado el sistema con make docker-compose-up.
2. Luego, entrá a la siguiente URL en tu navegador: [🔗 Link al gestor web](http://localhost:15672/)

### 📌 Credenciales por defecto:

- Usuario: guest
- Contraseña: guest

### ¿Qué nos permite hacer la interfaz del gestor?

Esta interfaz nos permite:

- Ver las colas activas.
- Inspeccionar mensajes.
- Observar cómo los controladores intercambian información.

## 📁 Archivos de entrada y salida

El sistema funciona con archivos de entrada y salida, se pasa a detallar el funcionamiento y ubicación de cada uno.

### Archivos de entrada

Residen en el directorio ".data/full_data", estos son los que envía el cliente junto con las queries, y le brindan al sistema los datos para realizar el procesamiento pedido.

Por motivos de tamaño excesivo no se pueden cargas los datasets directamente en el repositorio, por lo que deben cargarse manualmente.

Estos mismos pueden ser encontrados en el siguiente: [🔗 Link al dataset completo](https://www.kaggle.com/datasets/geraldooizx/g-coffee-shop-transaction-202307-to-202506/data)

### Archivos de salida

Las respuestas a las queries se generarán en archivos separados por cada una, que se crearán dentro del directorio '.results'.

Al finalizar la ejecución completa del procesamiento para todas las queries, dentro de ese directorio encontraremos el reporte final con los resultados para cada consulta realizada por el cliente.

## 💻 Script comparativo

Para validar el correcto funcionamiento del sistema, se cuenta con el script 'compare_results.py' dentro del directorio 'expected_output'.

Dentro del mismo directorio se encuentran cargadas las salidas esperadas para las queries del usuario, y el script se encarga de comparar dichos valores con los obtenidos en el directorio '.results'.

Para ejecutar el script comparativo se debe utilizar el siguiente comando:

```bash

python3 ./expected_output/compare_results.py --expected ./expected_output --actual ./.results/query_results

```

## 🎥 Desmotración de funcionamiento

[🔗 Link al video tutorial de funcionamiento](https://drive.google.com/drive/folders/1iDnXWh1Dd8fJBw4gxLcIzglYpP3rrnXQ?usp=sharing)
