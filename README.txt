/* 
 * Análisis Concurrente de un Log, usando una estrategia MapReduce con hilos y procesos
 * Uso: log archivo 
 * Lee un archivo de logs con 18 columnas las cuales tienen información
 * acerca de diferentes resultados encontrados en un log
 * El usuario del menú principal selecciona si desea realizar una consulta
 * o desea saliir del sistema
 * Si el usuario desea realizar una consulta, debe estar en el siguiente formato
 * $ columna, signo, valor ej. 5,>,30 y esto debe imprimir el número de trabajos que
 * se ejecutaron en más de 30 procesadores

 * Ejemplo: manager.c log10000.txt
 * Contenido:
   * Fuentes: 
     * analogh.c: Programa principal
     * analogh.h: Definiciones comunes
   * Datos de prueba:
     * log10000.txt
   * Makefile

 * Ver archivo de entrada "log10000.txt" como ejemplo del formato
 *
 * Adaptado por Alejandro Mayorga y Nicolás Miranda
 * 30 Abril del 2019
 */