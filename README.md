# Prueb Data Engineer ML

Elaborado por: Julian Gil

### Cómo ejecutar?

1. Crea un ambiente virtual con Python 3.11
2. Instala los requirements
    ```shell
    pip install requirements.txt
    ```
3. (Opcional) Ejecutar pruebas unitarias
    ```shell
    python -m unittest
    ```

4. Ejecutar el main
    ```shell
    python main.py
    ```

### Decisiones

- Se creó una clase abstracta llamada [ETL](etl.py) con el fin de configurar algunas responsabilidades genéricas para cualquier
proceso ETL, como definir el flujo de extract, transform y load. Además, en este caso, se definió de manera genérica la
configuración del logging.

- De esta clase abstracta se extiende la clase [ValuePropsEtl](etl.py#L36), que, para nuestro caso, contendrá toda la lógica de negocio
referente al ejercicio.

- La clase ValuePropsEtl recibirá un listado de [DataSources](datasource.py#L15), los cuales son objetos que contienen la información necesaria
para extraer datos desde el origen definido. Asimismo, recibe un parámetro que permite definir el rango en días que se
desea analizar. Todo esto se hace para evitar intervenir el código que contiene la lógica de negocio, la cual no debe
verse afectada por el origen de los data sources ni por los rangos de consulta.

- Para el método [_extract](etl.py#L44) se definió un algoritmo que permite extraer información desde cualquier archivo soportado [(CSV o
JSON)](datasource.py#L7). Además, en el caso de los archivos JSON, se realizará una normalización en caso de contener estructuras anidadas.

- El método [_transform](etl.py#L63) contiene toda la lógica de negocio necesaria para obtener la información solicitada en el
ejercicio.

- El método [_load](etl.py#L161) genera un archivo CSV y uno JSON que contienen el dataset que consumirá el modelo de machine learning (
ML), para facilitar su uso por parte del usuario final.