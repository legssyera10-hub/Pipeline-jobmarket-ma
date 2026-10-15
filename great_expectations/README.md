# Great Expectations

Les suites ne sont pas des fichiers JSON générés : elles sont écrites en Python,
versionnées et testées dans [src/quality/expectations.py](../src/quality/expectations.py)
(contexte GX éphémère). Les résultats de chaque exécution sont historisés dans la
table PostgreSQL `ops.controles_qualite`.
