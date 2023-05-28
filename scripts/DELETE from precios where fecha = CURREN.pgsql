DELETE from precios where fecha = CURRENT_DATE;


SELECT * FROM "precios_lista_larga" where fecha >= '2023-03-23' and fecha < '2023-03-24';

DELETE FROM precios_lista_larga a USING precios_lista_larga b WHERE a.id < b.id AND a.producto = b.producto;


SELECT DISTINCT ON (producto) producto, fecha, precio, id FROM "precios_lista_larga" WHERE fecha >= '2023-03-23' AND fecha < '2023-03-24';