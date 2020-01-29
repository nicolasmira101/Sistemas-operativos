/*
Nombre del programa: analogP

Autores: Alejandro Mayorga y Nicolás Miranda

Objetivo: Ademas de interiorizar y aplicar los conceptos de procesos e hilo en un entorno Unix, 	
	  se busca implementar un codigo cuya funcion consiste en encontrar coincidencias en un 
	  archivo de registros con valores ingresados por el usuario. Por ultimo se busca aplicar 
	  tambien la tecnica de MappReducers para atacar el problema.

Fecha de finalizacion: Domigno 17 de marzo del 2019.

Funciones presentes: validacionEntrada
 		     importadorMatriz
 		     consultador
 		     exportadorDatos
 		     exportadorDatos2
 		     estandarizador
 		     conversorArchivos
 		     contadorEntradasEnArchivo
 		     limpiarArchivos
 		     importarResultadosReducers

Anotaciones extra: todas las funciones fueros ideadas y escritas por mi, algunas inspiradas en ejemplos vistos en internet
*/
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <sys/time.h>

/*
Consulta: Estructura donde se guardara los datos necesarios de la consulta del usuario

*/
struct Consulta {
   int columna;
   char comando[3];
   float valor;
};

/*
Pares: Estructura donde se guardara los resultados de los Mappers (calve y valor)

*/
struct Pares {
   int clave;
   float valor;
};

int validacionEntrada(int argc, char *argv[]);
void importadorMatriz(const char *archivoMatriz,  float **matriz, int nfil);
int consultador(struct Consulta *consult, char *stringIn);
void exportadorDatos(struct Pares *pares, int tam, char *archivos);
void exportadorDatos2(int val, char *archivos);
void estandarizador(int filas, int *procesos,int  *filasXProcesos);
void conversorArchivos(char **archivosMappers ,int nombreArchivo, char identificador);
int contadorEntradasEnArchivo(const char *archivoRegistros );
void limpiarArchivos(char **archivosMappers,char **archivosReducers,int cantMappers,int cantReducers);
int importarResultadosReducers(char **archivosReducers, int cantReducers);
/*
Nota: Las funciones seran explicadas en su correspondiente implementacion.

*/
int main(int argc, char *argv[])
{
   

	if(validacionEntrada(argc, argv) == -1)
		perror(-1);
	
   	char *archivoLog = argv[1];//Variable donde se guardara el nombre del archivo de registros
	int numLineas = atoi(argv[2]);//Variable donde se alojara la cantidad de lineas en el archivo de registros
	int cantMappers = atoi(argv[3]);
	int cantReducers = atoi(argv[4]);
	int bitIntermedios = atoi(argv[5]);
	char mensajeUsuario[100];//Variable para guardar lo que el usuario escriba en la consola
	float **matrizDeLogs;
	/*
	matrizDeLogs: Variable donde se importara los datos del archivo de registros

	*/
	pid_t childpid; //Variable donde se guardara el ID del proceso hijo
	struct Consulta consult;//Variable para guardar la consulta del usuario
	int acumulador = 0;//Variable axiliar donde se realizara el trabajo de espera de los procesos hijos
	int filasXProceso[cantMappers];//Variable donde se guardara la cantidad de filas de la matriz de logs que cada mapper debe contar
	int archivosXProceso[cantReducers];//Variable donde se guardara la cantidad de reultados que cada reducer debe contar
	char **archivosMappers;//arreglo de los nombres delos archivos que los mappers usaran para guardar sus resultados
	char **archivosReducers;//arreglo de los nombres delos archivos que los reducers usaran para guardar sus resultados
	int salida = 0;//Variable para imprimir el resultado numerico final
	int primeraConsulta = 0;//Variable  auxiliar para saber si es la rimera consulta que se realiza
	struct timeval start, end;

	estandarizador(numLineas, &cantMappers, &filasXProceso);
	estandarizador(cantMappers, &cantReducers, &archivosXProceso);


	archivosMappers  = (char **)malloc(cantMappers*sizeof(char *));	
	archivosReducers  = (char **)malloc(cantReducers*sizeof(char *));		

	matrizDeLogs  = (float **)malloc(numLineas*sizeof(float *));		
	for (int i = 0; i < numLineas; i++) 
		matrizDeLogs[i]= (float *) malloc(18*sizeof(float));
	importadorMatriz(archivoLog,  matrizDeLogs, numLineas);
	printf("\e[1;1H\e[2J"); 

	/*
	While para que el usuario realice cuantas consultas desee

	*/
	while(1){
		
		if(bitIntermedios == 0 && primeraConsulta == 1)  limpiarArchivos(archivosMappers,archivosReducers,cantMappers,cantReducers);
		printf("\n");		
		printf("\n");
		printf("-------------------MENU--------------------\n");
		printf("1. Realizar una consulta\n");
		printf("2. Salir del sistema\n");
		printf("\n");
		printf("\n");
		scanf("%s", mensajeUsuario);
		if( strcmp(mensajeUsuario, "1") == 0 ){

			printf("\e[1;1H\e[2J"); 

			printf("Ingrese su consulta:\n");
			scanf("%s", mensajeUsuario);
			gettimeofday(&start, NULL);
		 	if(consultador(&consult, mensajeUsuario) == -1){
				printf("Consulta no reconocida\n");
				continue;
			}
			int k = 0;

			/*
			For para iniciar y ejecutar los procesos mappers

			*/
			for (int i = 0; i < cantMappers; i++) {	

				conversorArchivos(archivosMappers,i,'M');
				fflush(NULL);
				if ((childpid = fork()) < 0) {
					perror("fork:");
					exit(1);
				}
				
				/*
				inicio del codigo del procesos hijo

				*/
				if (childpid == 0) { 	

					struct Pares *pares;
					pares = (struct Pares *)malloc((numLineas)*sizeof(struct Pares ));
					for(int j = 0; j < numLineas; j++)
						(pares + j)->clave = -1;
						
					
					if(strcmp(consult.comando, ">") ==0){
						
						for( int j = filasXProceso[0] * i; j < (filasXProceso[0] * i) + filasXProceso[i]; j++){
							
							if(matrizDeLogs[j][consult.columna-1] > consult.valor){
								(pares + k)->clave = matrizDeLogs[j][0];
								(pares + k)->valor = matrizDeLogs[j][consult.columna-1];
								k = k + 1;
							}
							
						}
					}else if(strcmp(consult.comando, ">=") == 0 || strcmp(consult.comando, "=>") == 0){
						for( int j = filasXProceso[0] * i; j < (filasXProceso[0] * i) + filasXProceso[i]; j++){
							
							if(matrizDeLogs[j][consult.columna-1] >= consult.valor){
								(pares + k)->clave = matrizDeLogs[j][0];
								(pares + k)->valor = matrizDeLogs[j][consult.columna-1];
								k = k + 1;
							}
							
						}
					}else if(strcmp(consult.comando, "<") == 0){
						for( int j = filasXProceso[0] * i; j < (filasXProceso[0] * i) + filasXProceso[i]; j++){
							
							if(matrizDeLogs[j][consult.columna-1] < consult.valor){
								(pares + k)->clave = matrizDeLogs[j][0];
								(pares + k)->valor = matrizDeLogs[j][consult.columna-1];
								k = k + 1;
							}
							
						}
					}else if(strcmp(consult.comando, "<=") == 0 || strcmp(consult.comando, "=<") == 0){
						for( int j = filasXProceso[0] * i; j < (filasXProceso[0] * i) + filasXProceso[i]; j++){
							
							if(matrizDeLogs[j][consult.columna-1] <= consult.valor){
								(pares + k)->clave = matrizDeLogs[j][0];
								(pares + k)->valor = matrizDeLogs[j][consult.columna-1];
								k = k + 1;
							}
							
						}

					}else if(strcmp(consult.comando, "=") == 0){
						for( int j = filasXProceso[0] * i; j < (filasXProceso[0] * i) + filasXProceso[i]; j++){
							
							if(matrizDeLogs[j][consult.columna-1] == consult.valor){
								(pares + k)->clave = matrizDeLogs[j][0];
								(pares + k)->valor = matrizDeLogs[j][consult.columna-1];
								k = k + 1;
							}
							
						}

					}else if(strcmp(consult.comando, "!=") == 0){
						for( int j = filasXProceso[0] * i; j < (filasXProceso[0] * i) + filasXProceso[i]; j++){
							
							if(matrizDeLogs[j][consult.columna-1] != consult.valor){
								(pares + k)->clave = matrizDeLogs[j][0];
								(pares + k)->valor = matrizDeLogs[j][consult.columna-1];
								k = k + 1;
							}
							
						}

					}
					
					exportadorDatos(pares, (numLineas),(archivosMappers[i]));
					free(pares);
					exit(0);
					
					 
				}
			}
			while (wait(&acumulador) > 0){					1;
				
			}
			acumulador = 0;


			/*
			For para iniciar y ejecutar los procesos mappers

			*/
			for(int i = 0; i < cantReducers; i++){
			
				fflush(NULL);
				if ((childpid = fork()) < 0) {
					perror("fork:");
					exit(1);
				}
				conversorArchivos(archivosReducers,i,'R');

				/*
				inicio del codigo del procesos hijo

				*/
				if (childpid == 0) { 	
					
					int cant = 0;
					for(int j = archivosXProceso[0] * i; j < (archivosXProceso[0] * i) + archivosXProceso[i]; j++){
						cant = cant + contadorEntradasEnArchivo(archivosMappers[j]);
					}
					exportadorDatos2(cant,(archivosReducers[i]));
					exit(0);
				}
			}
			while (wait(&acumulador) > 0){					1;
				
			}
			
		

		salida = importarResultadosReducers(archivosReducers, cantReducers);
		printf("\e[1;1H\e[2J"); 
		gettimeofday(&end, NULL);
		/*
		Impresion de resultados finales

		*/
		printf("Salida: %d concurrencias en un tiempo de %d microsegundos usando procesos con archivos\n", salida,((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec)));
		printf("\n");		
		printf("\n");
		if(primeraConsulta == 0) primeraConsulta = 1;/// preguntar por como hacer el bit intermedios
		}else if( strcmp(mensajeUsuario, "2") == 0 ){
			if(primeraConsulta == 1)
				limpiarArchivos(archivosMappers,archivosReducers,cantMappers,cantReducers);

			/*
			Liberacion de memoria

			*/
			for(int i = 0; i < numLineas; i++)
				free(matrizDeLogs[i]);
			for(int i = 0; i < cantMappers;i++)
				free(archivosMappers[i]);
			
			for(int i = 0; i < cantReducers;i++)
				free(archivosReducers[i]);
			

			free(archivosMappers);
			free(archivosReducers);
			free(matrizDeLogs);
			printf("\e[1;1H\e[2J"); 
			printf("Gracias usar el programa!\n");

			break;
		}else{
			printf("\e[1;1H\e[2J"); 
			printf("Comando no reconocido\n");
		}
		
	}
   	return 0;
}

/*
Funcion: limpiarArchivos
Entradas: nombres de los archivos creados, tanto de mappers como de reducers y la cantidad de estos
Salidas: Ninguna mas alla de la eliminacion de dichos archivos
Funcionamiento: sencillamente elimina archivo por archivo

*/
void limpiarArchivos(char **archivosMappers,char **archivosReducers,int cantMappers,int cantReducers){

	for(int i = 0; i< cantMappers; i++)
		remove(archivosMappers[i]);

	for(int i = 0; i< cantReducers; i++)
		remove(archivosReducers[i]);
	
}

/*
Funcion: importarResultadosReducers
Entradas: Arreglo de nombres de los archivos creados por los reducers y la cantidad de estos
Salidas: un entero donde se guarda la cantidad de cuentas que se realizaron en todos los archivos
Funcionamiento: Sobre cada nombre de archivo, se abre el archivo y se extrae el numero en su interior.

*/
int importarResultadosReducers(char **archivosNombres, int cant){

	FILE *fp;
	char str[100];
	int salida = 0;
	for(int i = 0; i < cant; i++){
		fp = fopen(archivosNombres[i], "r");
		if(fp == NULL) {
	      		perror("Error abriendo el archivo");
	     		return(-1);
	   	}
		fgets(str, 100, fp);
		salida += atoi(str);
		fclose(fp);
	}
	return salida;
}

/*
Funcion: contadorEntradasEnArchivo
Entradas: Nombre del archivo donde estan los datos
Salidas: la cantidad e entradas  contadas en el archivo
Funcionamiento: Primero abre el archivo, luego cuenta hasta el final la cantidad de entradas que este tiene
*/
int contadorEntradasEnArchivo(const char *archivoRegistros ){
	int cant = 0;
	FILE *fp;
	fp = fopen(archivoRegistros, "r");
	int c;
	if(fp == NULL) {
      		perror("Error abriendo el archivo");
     		return(-1);
   	}
	while ((c = fgetc(fp)) != EOF)
    	{	
		if(c == '\n')
			cant = cant + 1;	
    	}
	fclose(fp);
	return cant;
}

/*
Funcion: consultador
Entradas: Una estrucuta donde se guardara los  datos de la consulta, un string que contiene la consulta sin tokenizar
Salidas: Un entero para saber si hubo exito o no guardando la consulta
Funcionamiento: A cada apartado de la estructura Consult, se le coloca su valor propio (columna, comando y valor),
	        luego se valida que dichos datos tengan sentido en el contexto del prolema

*/
int consultador(struct Consulta *consult, char *stringIn){
			
	consult->columna = atoi(strtok(stringIn, ","));
	strcpy(consult->comando,strtok(NULL, ","));	
	consult->valor = atof(strtok(NULL, ","));

	if(consult->columna > 18 || consult->columna < 1)
		return -1;
	if((strcmp(consult->comando, "=") == 0) || (strcmp(consult->comando, "!=") == 0) || (strcmp(consult->comando, ">") == 0) || (strcmp(consult->comando, "<") == 0) || (strcmp(consult->comando, ">=") == 0) || (strcmp(consult->comando, "<=") == 0)|| (strcmp(consult->comando, "=>") == 0) || (strcmp(consult->comando, "=<") == 0))
		return 1;
	else
		return -1;

	return 1;
}

/*
Funcion: validacionEntrada
Entradas: Un numero de argumentos y los argumentos
Salidas: Un entero para saber si los argumentos de entrada son validos
Funcionamiento: REvisa no solo que la cantidad de argumentos sea la correcta, tambien valida argumento por argumento

*/
int validacionEntrada(int argc, char *argv[]){


	int bit = -1;
	if(argc < 6){
		printf("Muy pocos argumentos\n");
		return bit;
	}else if(argc > 6){
		printf("Demasiados argumentos\n");
		return bit;
	}else{
		bit = 1;
	}
		bit = -1;
	
	if( access( argv[1], F_OK ) == -1 ){
		printf("Archivo inexistente\n");
		return bit;
	}

	if((atoi(argv[2]) <=0) || (atoi(argv[3]) <=0) || atoi(argv[5]) < 0 || (atoi(argv[4]) > (atoi(argv[3])))){

		printf("Ingrese numeros validos\n");
		return bit;
	}

	bit = 1;
	return bit;

}

/*
Funcion: importadorMatriz
Entradas: Nombre del archivo donde estan los registros y la cantidad de filas sobre las que se va a trabajar 
	  ademas de la matriz donde se alojara el resultado
Salidas: Ninguna mas alla de obtener los registros importados en la matriz
Funcionamiento: Primero se abre el archivo y luego elemento a elemento se importa en la matriz hasta el final

*/
void importadorMatriz(const char *archivoMatriz,  float **matriz, int nfil){


	int c = 0;
	char d [20];
	FILE *fp;
	fp = fopen(archivoMatriz, "r");
	if(fp == NULL) {
      		perror("Error abriendo el archivo");
     		return;
   	}
	int i;
	int j;
	for ( i = 0 ; i < nfil ; i ++ ) {
		for ( j = 0 ; j < 18 ; j ++ ) {
			fscanf(fp, "%s", d);
			c = fgetc(fp);
			
			matriz[i ][ j ] = atof(d);
			if( feof(fp) ) {
		   		break ;
			}
		
		}
		printf("\n");
	}	
	fclose(fp);
}

/*
Funcion: exportadorDatos (Para mappers)
Entradas: Una estrucuta pares donde estan los datos a exportar y el nombre del archivo donde se guardara el valor
Salidas: ninguna mas alla de los archivos creados con su contenido respectivo
Funcionamiento: Primero  se abre el archivo y luego se agrega en el archivo registro a registro los datos.

*/
void exportadorDatos(struct Pares *pares, int tam, char *archivos){

	
	FILE * fp;
	fp = fopen(archivos, "w+");
	if(fp == NULL) {
      		perror("Error abriendo el archivo");
     		return;
   	}
	for(int i = 0; i < tam; i++){
		if((pares + i)->clave != -1){
		fprintf(fp, "%d ", ((pares + i)->clave));
		fprintf(fp, "%f\n", ((pares + i)->valor));
		}
	}
	fclose(fp);
	
}

/*
Funcion: exportadorDatos2 (para reducers)
Entradas: valor a escribir en los archivos
Salidas: ninguna mas alla de los archivos creados con su contenido respectivo
Funcionamiento: Primero  se abre el archivo y luego se agrega en el archivo el valor.

*/
void exportadorDatos2(int val, char *archivos){

	FILE * fp;
	fp = fopen(archivos, "w+");
	if(fp == NULL) {
      		perror("Error abriendo el archivo");
     		return(-1);
   	}
	fprintf(fp, "%d ", (val));
	fclose(fp);
}

/*
Funcion: estandarizador
Entradas: Numero de elementos, cantidad de hilos/Procesos y un arreglo de elementos a desarrollar por dicho proceso/hilo
Salidas: el arreglo de elementos por proceso/hilo que se deben realizar
Funcionamiento: simplemente divide el numero de elementos a realizar en la cantidad de procesos/hilos y si sobra algo, 
		se le asigna al ultimo hilo/Proceso

*/
void estandarizador(int filas, int *procesos,int  *filasXProcesos){

	
	for(int i = 0; i < *procesos; i++)
		*(filasXProcesos + i) = (filas / *procesos);

	if((float)filas/ (float)*procesos < (float)1){
		*procesos = filas;
		for(int i = 0; i < *procesos; i++)
			*(filasXProcesos + i) = 1;
	}else if((float)(((float)filas / (float)*procesos) - (filas / *procesos) ) > (float)0){

		for(int i = 0; i < *procesos; i++){
			*(filasXProcesos + i) = (filas / *procesos);
		}	
		*(filasXProcesos + (*procesos -1)) = *(filasXProcesos + (*procesos -1)) + filas - (*procesos * (filas / *procesos));
		
	} 

	return;
}


/*
Funcion: conversorArchivos
Entradas: Arreglo de nombres que se va a inicializar, el nombre que se la va a colocar y el identificador unico de este
Salidas: nada mas alla de los nombres inicializados
Funcionamiento: Se busca el nombredel archivo en cuestion y se inicializa con un valor unico relacionado al proceso que lo va a usar

*/
void conversorArchivos(char **archivosMappers ,int nombreArchivo,char identificador){

	int nombreArchivo2 = nombreArchivo;
        int n = floor(log10(nombreArchivo + 1)) + 1 + 6;
        archivosMappers[nombreArchivo]  =  (char *) malloc((n)*sizeof(char));
        for (int i = 0; i < n; i++, nombreArchivo /= 10 ){
        	archivosMappers[nombreArchivo2][i] = (nombreArchivo % 10) + 48;
	}
	archivosMappers[nombreArchivo2][n - 1] ='\0';
	archivosMappers[nombreArchivo2][n - 2] = 't';
	archivosMappers[nombreArchivo2][n - 3] = 'x';
	archivosMappers[nombreArchivo2][n - 4] = 't';
	archivosMappers[nombreArchivo2][n - 5] = '.';
	archivosMappers[nombreArchivo2][n - 6] = identificador;
	
}

