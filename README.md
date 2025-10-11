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

## 🛠️ Informe de Diseño

El informe técnico incluye:
- Decisiones de diseño.
- Implementación de cada ejercicio.
- Protocolo de comunicación.
- Mecanismos de concurrencia utilizados.
- Instrucciones de ejecución.

[📑 Acceso al informe](./docs/Informe-G9-Diseño.pdf).

## 🧰 Guía rápida de uso con `Makefile`

### 🚀 Ejecución del Sistema con Docker Compose

En este TP se utiliza **Docker Compose** para levantar todos los componentes del sistema distribuido:
- El **cliente** que realiza las queries.
- El **servidor** que las recibe.
- Todos los **nodos** del sistema distribuido.
- El **middleware**.

#### ⚙️ Construir las imágenes Docker

Antes de levantar el sistema, es posible (aunque no obligatorio) construir manualmente todas las imágenes Docker definidas dentro del proyecto.

```bash

make docker-build-image

```

🔧 Este comando busca automáticamente todos los Dockerfile dentro de src/, los construye y los etiqueta con el nombre del directorio correspondiente.

💡 No es necesario ejecutarlo manualmente, ya que make docker-compose-up lo ejecuta automáticamente antes de levantar los contenedores.

#### ▶️ Levantar todo el sistema

```bash

make docker-compose-up

```

✅ Este comando pone en marcha todos los servicios del sistema distribuido (cliente, servidor, nodos y middleware).

Además, se asegura de reconstruir imágenes si detecta cambios y elimina contenedores huérfanos de ejecuciones anteriores.

#### ⏹️ Apagar todo el sistema

```bash

make docker-compose-down

```

❌ Detiene todos los servicios activos y elimina los contenedores, liberando los recursos utilizados.

El sistema quedará completamente detenido y en un estado limpio.

#### 📜 Ver los logs del sistema

```bash

make docker-compose-logs

```

👀 Muestra en consola los últimos 500 registros de cada contenedor y mantiene el seguimiento en tiempo real (-f).

Ideal para monitorear el comportamiento de los componentes durante la ejecución.

#### 🔎 Filtrar logs de un contenedor específico

```bash

make docker-compose-logs | grep '<nombre_del_contenedor>'

```

👉 Permite filtrar los logs para enfocarse en un componente en particular, por ejemplo los filters del sistema.

##### Ejemplo

```bash

make docker-compose-logs | grep 'filter'

```

### 🧪 Testing

El Makefile también incluye herramientas de testing para verificar el correcto funcionamiento del sistema distribuido.

#### 🧱 Tests unitarios de funcionamiento del Middleware

```bash

make unit-tests

```

🔍 Ejecuta los tests unitarios definidos con pytest en modo detallado (--verbose).

Estos tests suelen enfocarse en el middleware u otras partes específicas del sistema.

#### 🔗 Tests de integración

```bash

make integration-tests

```

🧩 Compara las salidas generadas por el sistema '.results/query_results/' contra las salidas esperadas definidas en 'integration-tests/data/expected_output/'.

#### 🧪 Tests de propagación EOF

```bash

make eof-propagation-tests

```

Estos tests se encargan de validar la correcta propagación de los EOF a lo largo del funcionamiento del sistema distribuido.

De este modo se puede corroborar la implementación adecuada del mecanismo de finalización de procesamientos, asegurando que el modelo de concurrencia y comunicación elegido resulta óptimo para el esquema multi-cliente implementado.

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

Las respuestas a las queries se generarán en archivos separados por cada una, que se crearán dentro del directorio '.results/query_results'.

Al finalizar la ejecución completa del procesamiento para todas las queries, dentro de ese directorio encontraremos el reporte final con los resultados para cada consulta realizada por el cliente.

## 🎥 Desmotración de funcionamiento

[🔗 Link al video tutorial de funcionamiento](https://drive.google.com/drive/folders/1iDnXWh1Dd8fJBw4gxLcIzglYpP3rrnXQ?usp=sharing)
