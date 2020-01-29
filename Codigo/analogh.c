/*
Nombre del programa: analogH

Autores: Alejandro Mayorga y Nicolás Miranda.

Objetivo: Ademas de interiorizar y aplicar los conceptos de procesos e hilos en un entorno Unix, 	
	  se busca implementar un codigo cuya funcion consiste en encontrar coincidencias en un 
	  archivo de registros con valores ingresados por el usuario. Por ultimo se busca aplicar 
	  tambien la tecnica de MappReducers para atacar el problema.

Fecha de finalizacion: Domigno 17 de marzo del 2019.

Funciones presentes: validacionEntrada
 		     importadorMatriz
 		     consultador
 		     estandarizador
 		     *cuentaConcurrenciasM
 		     *cuentaConcurrenciasR

Anotaciones extra: todas las funciones fueros ideadas y escritas por mi, algunas inspiradas en ejemplos vistos en internet
*/
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <sys/time.h>

/*
matrizDeLogs: Variable donde se importara los datos del archivo de registros

*/
float **matrizDeLogs;

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
/*
ParametrosM: Estructura donde que se usara para pasar por parametro a los hilos usados como mappers
contiene un rango donde cada hilo operara, un conjunto de pares donde se guardaran los daors y los datos especificos de la consulta.

*/
struct ParametrosM {
   	
   	int desde;
   	int hasta;
	struct Pares *pares;
	struct Consulta *cons;	
};
/*
ParametrosR: Estructura donde que se usara para pasar por parametro a los hilos usados como reducers
contiene un rango donde cada hilo operara, un conjunto de parametrosM donde estan los elementos a contar 
*/
struct ParametrosR {
   	
   	int desde;
   	int hasta;
	struct ParametrosM *parametroM;
};

/*
Nota: Las funciones seran explicadas en su correspondiente implementacion.

*/
int validacionEntrada(int argc, char *argv[]);
void importadorMatriz(const char *archivoMatriz, int nfil);
int consultador(struct Consulta *consult, char *stringIn);
void estandarizador(int filas, int *procesos,int  *filasXProcesos);
void *cuentaConcurrenciasM(struct ParametrosM *params);
void *cuentaConcurrenciasR(struct ParametrosR *params);


int main(int argc, char *argv[])
{
   
	fflush(NULL);
 
	if(validacionEntrada(argc, argv) == -1)
		perror(-1);

	int rc;//Variable para guardar el resultado de la inicializacion de los hilos
   	char *archivoLog = argv[1];//Variable donde se guardara el nombre del archivo de registros
	int numLineas = atoi(argv[2]);//Variable donde se alojara la cantidad de lineas en el archivo de registros
	int cantMappers = atoi(argv[3]);
	int cantReducers = atoi(argv[4]);
	char mensajeUsuario[100];//Variable para guardar lo que el usuario escriba en la consola
	struct Consulta consult;//Variable para guardar la consulta del usuario
	int filasXHilo[cantMappers];//Variable donde se guardara la cantidad de filas de la matriz de logs que cada mapper debe contar
	int resultadosXHilo[cantReducers];//Variable donde se guardara la cantidad de reultados que cada reducer debe contar
	int salida = 0;//Variable para imprimir el resultado numerico final
	int *resultados = (int *) malloc(sizeof(int));//Variable para guardar lo que retorne cada Mapper
	struct timeval start, end;
	*resultados = 0;
	int primeraConsulta = 0;
	estandarizador(numLineas, &cantMappers, &filasXHilo);
	estandarizador(cantMappers, &cantReducers, &resultadosXHilo);
	struct ParametrosM paramsM[cantMappers];
	struct ParametrosR paramsR[cantReducers];
	pthread_t mappers[cantMappers];//Arreglo de hilos mappers
	pthread_t reducers[cantReducers];//Arreglo de hilos reducers
	int aux = 0;//Variable auxiliar
  	
	printf("Cantidad de mappers %d y de reducers %d\n",cantMappers, cantReducers);

	matrizDeLogs  = (float **)malloc(numLineas*sizeof(float *));		
	for (int i = 0; i < numLineas; i++) 
		matrizDeLogs[i]= (float *) malloc(18*sizeof(float));
	importadorMatriz(archivoLog, numLineas);
	//printf("\e[1;1H\e[2J"); 

	/*
	While para que el usuario realice cuantas consultas desee

	*/
	while(1){
		
		printf("-------------------MENU--------------------\n");
		printf("1. Realizar una consulta\n");
		printf("2. Salir del sistema\n");
		printf("\n");
		printf("\n");
		scanf("%s", mensajeUsuario);
		if( strcmp(mensajeUsuario, "1") == 0 ){

			//printf("\e[1;1H\e[2J"); 

			printf("Ingrese su consulta:\n");
			scanf("%s", mensajeUsuario);
			gettimeofday(&start, NULL);
		 	if(consultador(&consult, mensajeUsuario) == -1){
				printf("Consulta no reconocida\n");
				continue;
			}
			int k = 0;
			/*
			For para iniciar y ejecutar los hilos mappers

			*/
			for (int i = 0; i < cantMappers; i++) {	

				fflush(NULL);
				paramsM[i].desde = i * filasXHilo[0];
				paramsM[i].hasta = (filasXHilo[0] * i) + filasXHilo[i];
				aux = paramsM[i].hasta - paramsM[i].desde;
				paramsM[i].pares = (struct Pares *)malloc((aux + 1)*sizeof(struct Pares ));
				for(int j = 0; j < aux + 1 ; j++)
					paramsM[i].pares[j].clave = -1;

				paramsM[i].cons = &consult;
				rc = pthread_create(&mappers[i], NULL, (void*)cuentaConcurrenciasM, (void *) &paramsM[i]);
				if (rc) {
					perror("Error de hilos");
					exit(-1);
				}
				

			}
			
			for(int j = 0; j < cantMappers; j++)					
				pthread_join(mappers[j],NULL);

			/*
			For para iniciar y ejecutar los hilos reducers

			*/
			for(int i = 0; i < cantReducers; i++){
			
				fflush(NULL);
				paramsR[i].desde = i * resultadosXHilo[0]; 
				paramsR[i].hasta = (resultadosXHilo[0] * i) + resultadosXHilo[i];
				paramsR[i].parametroM = &paramsM;
				rc = pthread_create(&reducers[i], NULL, (void*)cuentaConcurrenciasR, (void *) &paramsR[i]);
				if (rc) {
					perror("Error de hilos");
					exit(-1);
				}

			}
			
			for(int j = 0; j < cantReducers; j++){		
				pthread_join(reducers[j], (void **)&resultados);
				salida = salida + *resultados;
			}
			
			/*
			Impresion de resultados finales

			*/
		
			//printf("\e[1;1H\e[2J"); 
			gettimeofday(&end, NULL);
			printf("Salida: %d concurrencias en un tiempo de %d microsegundos usando hilos\n", salida,((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec)));
			printf("\n");		
			printf("\n");
			salida = 0;

		}else if( strcmp(mensajeUsuario, "2") == 0 ){	
			
			/*
			Liberacion de memoria

			*/
			for(int i = 0; i < numLineas; i++)
				free(matrizDeLogs[i]);
			for(int i = 0; i < cantMappers;i++)
				free(paramsM[i].pares);

			free(matrizDeLogs);
			free(resultados);
			//printf("\e[1;1H\e[2J"); 
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
	if(argc < 5){
		printf("Muy pocos argumentos\n");
		return bit;
	}else if(argc > 5){
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

	if((atoi(argv[2]) <=0) || (atoi(argv[3]) <=0) || (atoi(argv[4]) > (atoi(argv[3])))){

		printf("Ingrese numeros validos\n");
		return bit;
	}

	bit = 1;
	return bit;

}
/*
Funcion: importadorMatriz
Entradas: Nombre del archivo donde estan los registros y la cantidad de filas sobre las que se va a trabajar
Salidas: Ninguna mas alla de obtener los registros importados en la matriz
Funcionamiento: Primero se abre el archivo y luego elemento a elemento se importa en la matriz hasta el final

*/
void importadorMatriz(const char *archivoMatriz, int nfil){


	int c = 0;
	char d [20];
	FILE *fp;
	fp = fopen(archivoMatriz, "r");
	if(fp == NULL) {
      		perror("Error abriendo archivo");
     		return;
   	}
	int i;
	int j;
	for ( i = 0 ; i < nfil ; i ++ ) {
		for ( j = 0 ; j < 18 ; j ++ ) {
			fscanf(fp, "%s", d);
			c = fgetc(fp);
			
			matrizDeLogs[i ][ j ] = atof(d);
			if( feof(fp) ) {
		   		break ;
			}
		
		}
		printf("\n");
	}	
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
Funcion: cuentaConcurrenciasM
Entradas: ParametrosM (ver definicion de estructura)
Salidas: ParametrosM actualizado con el resultado de la cuentapor parte de los mappers
Funcionamiento: Dependiendo de el comando de la consulta (ver estructura de consult), se iterara sobre el rango dado
		en los parametros y se contara la cantidad de coincidencias con el valor de la consulta.

*/

void *cuentaConcurrenciasM(struct ParametrosM *params){

	fflush(NULL);
	int k = 0;
	if(strcmp(params->cons->comando, ">") ==0){
		for( int j = params->desde; j < params->hasta; j++){
							
			if(matrizDeLogs[j][params->cons->columna-1] > params->cons->valor){

				params->pares[k].clave = matrizDeLogs[j][0];
				params->pares[k].valor = matrizDeLogs[j][params->cons->columna-1];
				k = k + 1;
			}
							
		}
	}else if(strcmp(params->cons->comando, ">=") == 0 || strcmp(params->cons->comando, "=>") == 0){
		for( int j = params->desde; j < params->hasta; j++){
							
			if(matrizDeLogs[j][params->cons->columna-1] >= params->cons->valor){
				params->pares[k].clave = matrizDeLogs[j][0];
				params->pares[k].valor = matrizDeLogs[j][params->cons->columna-1];
				k = k + 1;
			}
							
		}
	}else if(strcmp(params->cons->comando, "<") == 0){
		for( int j = params->desde; j < params->hasta; j++){
							
			if(matrizDeLogs[j][params->cons->columna-1] < params->cons->valor){
				params->pares[k].clave = matrizDeLogs[j][0];
				params->pares[k].valor = matrizDeLogs[j][params->cons->columna-1];
				k = k + 1;
			}
							
		}
	}else if(strcmp(params->cons->comando, "<=") == 0 || strcmp(params->cons->comando, "=<") == 0){
		for( int j = params->desde; j < params->hasta; j++){
							
			if(matrizDeLogs[j][params->cons->columna-1] <= params->cons->valor){
				params->pares[k].clave = matrizDeLogs[j][0];
				params->pares[k].valor = matrizDeLogs[j][params->cons->columna-1];
				k = k + 1;
			}
							
		}

	}else if(strcmp(params->cons->comando, "=") == 0){
		for( int j = params->desde; j < params->hasta; j++){
							
			if(matrizDeLogs[j][params->cons->columna-1] == params->cons->valor){
				params->pares[k].clave = matrizDeLogs[j][0];
				params->pares[k].valor = matrizDeLogs[j][params->cons->columna-1];
				k = k + 1;
			}
							
		}

	}else if(strcmp(params->cons->comando, "!=") == 0){
		for( int j = params->desde; j < params->hasta; j++){
							
			if(matrizDeLogs[j][params->cons->columna-1] != params->cons->valor){
				params->pares[k].clave = matrizDeLogs[j][0];
				params->pares[k].valor = matrizDeLogs[j][params->cons->columna-1];
				k = k + 1;
			}
							
		}

	}
	
	pthread_exit();

}

/*
Funcion: cuentaConcurrenciasR
Entradas: ParametrosR (ver definicion de estructura)
Salidas: Una variable donde se almaceno la cantidad de cuentas realizadas
Funcionamiento: El hilo recibira contara las entradas de las estructuras en el parametro (ver definiciones de dichas estructuras) y retornara 
		la cantidad que cuente.s

*/

void *cuentaConcurrenciasR(struct ParametrosR *params){

	fflush(NULL);
	int *cuenta = (int *) malloc(sizeof(int));
	*cuenta = 0;
	int k =0;
	
	for( int j = params->desde; j < params->hasta; j++){
		while(params->parametroM[j].pares[k].clave != -1){
			*cuenta = *cuenta + 1;
			k++;				
		}
		k = 0;
	}
	pthread_exit(cuenta);

}
