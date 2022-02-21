-- crear base de datos cultura en posgresql
CREATE DATABASE "cultura";

DROP TABLE IF EXISTS "general_data";
DROP TABLE IF EXISTS "cantidades";
DROP TABLE IF EXISTS "info_cines";

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

CREATE TABLE IF NOT EXISTS "general_data" (
  "cod_localidad" INT GENERATED BY DEFAULT AS IDENTITY,
  "id_provincia" INT GENERATED BY DEFAULT AS IDENTITY,
  "id_departamento" INT GENERATED BY DEFAULT AS IDENTITY,
  "categoria" varchar(64) NOT NULL,
  "provincia" varchar(64) NOT NULL,
  "localidad" varchar(64) DEFAULT NULL,
  "nombre" varchar(64) DEFAULT NULL,
  "domicilio" text DEFAULT NULL,
  "cod_postal" varchar(8) DEFAULT NULL,
  "num_telefono" varchar(20) DEFAULT NULL,
  "mail" varchar(320) DEFAULT NULL,
  "web" text DEFAULT NULL,
  PRIMARY KEY ("cod_localidad", "id_provincia", "id_departamento")
);

-- generar una tabla con la siguiente información:
--  Cantidad de registros totales por categoría
--  Cantidad de registros totales por fuente
--  Cantidad de registros por provincia y categoría
-- ATENCION: se implemento una decision de diseño explicado en el archivo 
-- main.py segundo docs string.

CREATE TABLE IF NOT EXISTS "cantidades" (
    "id" int GENERATED ALWAYS AS IDENTITY,
    "categoria" varchar(64) NOT NULL,
    "cantidad_registros_categoria" smallint NOT NULL,
    "provincia" varchar(64) NOT NULL,
    "cantidad_registros_prov_categ" smallint NOT NULL,
    "fuente" varchar(100) NOT NULL,
    "cantidad_registros_fuente" smallint NOT NULL,
    PRIMARY KEY ("id")
);


-- crear una tabla que contenga:
--  Provincia
--  Cantidad de pantallas
--  Cantidad de butacas
--  Cantidad de espacios INCAA

CREATE TABLE IF NOT EXISTS "info_cines" (
    "id" int GENERATED ALWAYS AS IDENTITY,
    "provincia" varchar(64) NOT NULL,
    "cantidad_pantallas" smallint NOT NULL DEFAULT 0,
    "cantidad_butacas" smallint NOT NULL DEFAULT 0,
    "cantidad_espacios_incaa" smallint NOT NULL DEFAULT 0,
    PRIMARY KEY ("id")
);
