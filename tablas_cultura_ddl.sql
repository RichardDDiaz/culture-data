DROP SCHEMA IF EXISTS tablas_cultura CASCADE;
CREATE SCHEMA IF NOT EXISTS tablas_cultura;


DROP TABLE IF EXISTS tablas_cultura.centros_cultura;
DROP TABLE IF EXISTS tablas_cultura.cantidades;
DROP TABLE IF EXISTS tablas_cultura.info_cines_provincia;

-- crear tabla "general_data" en la base de datos cultura 
-- con las siguientes columnas:
--  cod_localidad
--  id_provincia
--  id_departamento
--  categoría
--  provincia
--  localidad
--  nombre
--  domicilio
--  código postal
--  número de teléfono
--  mail
--  web

CREATE TABLE IF NOT EXISTS tablas_cultura.centros_cultura (
  id_centro_cultural INT GENERATED ALWAYS AS IDENTITY (START WITH 1) PRIMARY KEY,
  cod_localidad INT NOT NULL,
  id_provincia INT NOT NULL,
  id_departamento INT NOT NULL,
  categoria varchar(200) NOT NULL,
  provincia varchar(200) NOT NULL,
  localidad varchar(200) DEFAULT NULL,
  nombre varchar(200) DEFAULT NULL,
  domicilio text DEFAULT NULL,
  codigo_postal varchar(8) DEFAULT NULL,
  numero_de_telefono varchar(20) DEFAULT NULL,
  mail varchar(320) DEFAULT NULL,
  web text DEFAULT NULL,
  fecha_carga TIMESTAMP DEFAULT NULL
);


-- generar una tabla con la siguiente información:
--  Cantidad de registros totales por categoría
--  Cantidad de registros totales por fuente
--  Cantidad de registros por provincia y categoría
-- ATENCION: se implemento una decision de diseño explicado en el archivo 
-- main.py segundo docs string.

CREATE TABLE IF NOT EXISTS tablas_cultura.cantidades (
    id_cantidad int GENERATED ALWAYS AS IDENTITY (START WITH 1) PRIMARY KEY,
    categoria varchar(64) NOT NULL,
    cantidad_registros_categoria smallint NOT NULL,
    provincia varchar(64) NOT NULL,
    cantidad_registros_prov_categ smallint NOT NULL,
    fuente varchar(100) NOT NULL,
    cantidad_registros_fuente smallint NOT NULL,
    fecha_carga TIMESTAMP DEFAULT NULL
);


-- crear una tabla que contenga:
--  Provincia
--  Cantidad de pantallas
--  Cantidad de butacas
--  Cantidad de espacios INCAA

CREATE TABLE IF NOT EXISTS tablas_cultura.info_cines_provincia (
    id_provincia int GENERATED ALWAYS AS IDENTITY (START WITH 1) PRIMARY KEY,
    provincia varchar(64) NOT NULL,
    cantidad_de_pantallas smallint NOT NULL DEFAULT 0,
    cantidad_de_butacas INT NOT NULL DEFAULT 0,
    cantidad_de_espacios_incaa smallint NOT NULL DEFAULT 0,
    fecha_carga TIMESTAMP DEFAULT NULL
);


-- triggers que modifica la columna "fecha_carga" de las 3 tablas
-- antes de insertarlas

CREATE OR REPLACE FUNCTION insert_currect_date()
RETURNS TRIGGER AS $insert_currect_date$
DECLARE
BEGIN 
    NEW.fecha_carga = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$insert_currect_date$ LANGUAGE plpgsql;


CREATE TRIGGER centros_cultura_insert_currect_date 
  BEFORE INSERT OR UPDATE ON tablas_cultura.centros_cultura
  FOR EACH ROW
  EXECUTE PROCEDURE insert_currect_date();

CREATE TRIGGER cantidades_insert_currect_date 
  BEFORE INSERT OR UPDATE ON tablas_cultura.cantidades
  FOR EACH ROW
  EXECUTE PROCEDURE insert_currect_date();

CREATE TRIGGER info_cines_provincia_insert_currect_date 
  BEFORE INSERT OR UPDATE ON tablas_cultura.info_cines_provincia
  FOR EACH ROW
  EXECUTE PROCEDURE insert_currect_date();

