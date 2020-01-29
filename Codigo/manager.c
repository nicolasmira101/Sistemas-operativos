
/*
Nombre del programa: manager

Autores: Alejandro Mayorga.
	 Nicolas Miranda

Objetivo: Ademas de interiorizar y aplicar los conceptos de procesos e hilo en un entorno Unix, 	
	  se busca implementar un codigo cuya funcion consiste en encontrar coincidencias en un 
	  archivo de registros con valores ingresados por el usuario. Por ultimo se busca aplicar 
	  tambien la tecnica de MappReducers para atacar el problema.

Fecha de finalizacion: Martes 16 de abril de 2019.

Funciones presentes: validacionEntrada
 		     importadorMatriz
 		     consultador
 		     exportadorDatos
 		     estandarizador
 		     importarResultadosReducers

Anotaciones extra: todas las funciones fueros ideadas y escritas por nosotros, algunas inspiradas en ejemplos vistos en internet
*/
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <sys/time.h>
#include <fcntl.h>
#include <signal.h>
#include "estructurasAux.h"


int validacionEntrada(int argc, char *argv[]);
void importadorMatriz(const char *archivoMatriz,  float **matriz, int nfil);
int consultador(struct Consulta *consult, char *stringIn);
void exportadorDatos(struct Pares *pares, int tam, char *archivos);
void estandarizador(int filas, int *procesos,int  *filasXProcesos);
int cuentaConcurrenciasM(struct ParametrosM *params, float **matrizDeLogs, struct Pares pares[]);
void copiadorDeStrings(char origen[] , char *destino, int tam);
void conversorArchivos(char **archivosMappers ,int nombreArchivo);
/*
Nota: Las funciones seran explicadas en su correspondiente implementacion.

*/






//----------------------------------------------------Variables globales-----------------------------------------------
int pipeMappers[2];
int pipeReducers[2];
int finalizacionDeProceso = 0;
//---------------------------------------------------------------------------------------------------------------------









//----------------------------------------------------Implementaciones de manejadores de señales-----------------------
typedef void (*sighandler_t)(int);

//Signal handlers para mapper
sighandler_t signalHandlerMappers (void)
{

	1;
  
}
//Signal handlers para mapper
sighandler_t signalHandlerFinalizacionMapper (void)
{

	finalizacionDeProceso = 1;
  
}

//Signal handlers para reducer
sighandler_t signalHandlerFinalizacionReducer (void)
{

  	finalizacionDeProceso = 1;
  
}
//-------------------------------------------------------------------------------------------------------------------------------------








//----------------------------------------------------Inicio del programa-------------------------------------------------------

int main(int argc, char *argv[])
{
   	

	if(validacionEntrada(argc, argv) == -1){
		return -1;
	}


//----------------------------------------------------Declaracion de variables-------------------------------------------------------

   	char *archivoLog = argv[1];		//Variable donde se guardara el nombre del archivo de registros
	int numLineas = atoi(argv[2]);		//Variable donde se alojara la cantidad de lineas en el archivo de registros
	int cantMappers = atoi(argv[3]);
	int cantReducers = atoi(argv[4]);
	int bitIntermedios = atoi(argv[5]);
	char mensajeUsuario[100];		//Variable para guardar lo que el usuario escriba en la consola
	float **matrizDeLogs;			//variable matriz donde se importara la matriz del archivo
	struct Consulta consult;		//Variable para guardar la consulta del usuario
	int acumulador = 0;			//Variable axiliar donde se realizara el trabajo de espera de los procesos hijos
	int filasXProceso[cantMappers];		//Variable donde se guardara la cantidad de filas de la matriz de logs que cada mapper debe contar
	int mappersXProceso[cantReducers];	//Variable donde se guardara la cantidad de reultados que cada reducer debe contar
	int salida = 0;				//Variable para imprimir el resultado numerico final
	int primeraConsulta = 0;		//Variable  auxiliar para saber si es la rimera consulta que se realiza
	int cuantos;				//Variable axuliar para saber si hubo fallos en las lecturas y aprturas de pipes
	int aux = 0;				//Variable para almacenar valores auxiliares
	struct timeval start, end;
	char **nombresDePipesR;			//Arreglo con los nombres delos pipes

	estandarizador(numLineas, &cantMappers, &filasXProceso);
	estandarizador(cantMappers, &cantReducers, &mappersXProceso);

	struct ParametrosM paramsM[cantMappers];

	pid_t idPadre = getpid();
	pid_t childpidMappers[cantMappers]; 	//Variable donde se guardara el ID del proceso hijo mapper
	pid_t childpidReducers[cantReducers]; 	//Variable donde se guardara el ID del proceso hijoreducer

	matrizDeLogs  = (float **)malloc(numLineas*sizeof(float *));
	for (int i = 0; i < numLineas; i++) 
		matrizDeLogs[i]= (float *) malloc(18*sizeof(float));

	nombresDePipesR  = (char **)malloc(cantReducers *sizeof(char *));
	for(int i = 0; i < cantReducers; i++)
		conversorArchivos(nombresDePipesR,  i);

	importadorMatriz(archivoLog,  matrizDeLogs, numLineas);
	pipe(pipeMappers);
	pipe(pipeReducers);

	printf("\e[1;1H\e[2J");
//-----------------------------------------------------------------------------------------------------------------------------------









//----------------------------------------------------For para iniciar y ejecutar los procesos reducers------------------------------

	for(int i = 0; i < cantReducers; i++){
		
		int cuentaR = 0;
		fflush(NULL);
		if ((childpidReducers[i] = fork()) < 0) {
			perror("fork:");
			exit(1);
		}

		/*
		inicio del codigo del procesos reducer

		*/
		if (childpidReducers[i] == 0) { 
			
			signal(SIGUSR2, (sighandler_t) signalHandlerFinalizacionReducer);
			close(pipeReducers[0]);
			int cuantos;
			int cant = 0;
			int fd;
			struct Pares pares;
			mode_t fifo_mode = S_IRUSR | S_IWUSR | 0777;



			if (mkfifo (nombresDePipesR[i] , fifo_mode) == -1) {
				perror("~~nom2 mkfifo\n");
				exit(1);
			}
			while(1){
				fd = open (nombresDePipesR[i], O_RDONLY );
			  	if (fd == -1) {
					perror("~~pipe1 del reducer\n");
					printf("~~Se volvera a intentar despuesR\n");
					sleep(5);        
			   	}else{
					break;
				}
			}

			while(1){
				if(finalizacionDeProceso == 1){
					break;
				}

				for(int j = 0; j < mappersXProceso[i]; j++){
					while(salida < mappersXProceso[i]){

						if(finalizacionDeProceso == 1){
							
							break;
						}
						cuantos = read (fd, &pares, sizeof(struct Pares));
						if(pares.clave != -1)
							cant++;
						else
							salida++;

					  	if (cuantos == -1) {
							perror("~~proceso lector:");
							if(finalizacionDeProceso == 1)
								break;
					  		exit(1);
					  	}

						if(bitIntermedios == 1){
							if(pares.clave != -1 && finalizacionDeProceso == 0){
								printf("~~en el reducer %d, Clave: %d Valor: %f\n", i, pares.clave, pares.valor);
								
							}
						}
					}


				}
				
				write(pipeReducers[1], &cant, sizeof(int));
				cant = 0;
				salida = 0;
			}

			/*
			Liberacion de memoria y de pipes

			*/

			unlink(nombresDePipesR[i]);

			for(int j = 0; j < cantReducers; j++)
				free(nombresDePipesR[j]);
			free(nombresDePipesR);

			for(int j = 0; j < numLineas; j++)
				free(matrizDeLogs[j]);

			free(matrizDeLogs);
			close(fd);
			close(pipeReducers[1]);
			//printf("\e[1;1H\e[2J");
			exit(0);
		}
	}

//-------------------------------------------------------------------------------------------------------------------------------------









//----------------------------------------------------For para iniciar y ejecutar los procesos mapper----------------------------------

	for (int i = 0; i < cantMappers; i++) {	

		fflush(NULL);
		if ((childpidMappers[i] = fork()) < 0) {
				perror("fork:");
				exit(1);
		}
		/*
		inicio del codigo del procesos mapper

		*/
		if(childpidMappers[i] == 0){

			signal(SIGUSR1, (sighandler_t) signalHandlerMappers);
			signal(SIGUSR2, (sighandler_t) signalHandlerFinalizacionMapper);
			close(pipeMappers[1]);
			int cuantos;
			int reducerAlQueEscribira;
			struct ParametrosM parametroParaPipeMapper;
			struct Pares pares [numLineas];
			int fd;

			for(int j = 0; j < cantReducers; j++){
				if(i >= j * mappersXProceso[0] && i < (j * mappersXProceso[j]) + mappersXProceso[j]){
					reducerAlQueEscribira = j;
					while(1){ 
						fd = open(nombresDePipesR[j], O_WRONLY );
						if (fd == -1) {
							perror("~pipe");
							sleep(5);        
						}else{
							break;
						}
					}
					break;
				}
			}

			while(1){
				pause();
				if(finalizacionDeProceso == 1)
					break;
				cuantos = read (pipeMappers[0], &parametroParaPipeMapper, sizeof(struct ParametrosM));

			  	if (cuantos == -1) {
			     		perror("~proceso mapper:");
			     		exit(1);
			  	}
				for(int j = 0; j < numLineas ; j++)
					pares[j].clave = -1;

				cuantos = cuentaConcurrenciasM(&parametroParaPipeMapper, matrizDeLogs, pares);
				for(int k = 0; k < numLineas; k++){
					if(pares[k].clave != -1){
						write(fd, &pares[k], sizeof(struct Pares));
					}else{
						write(fd, &pares[k], sizeof(struct Pares));
						break;
					}
				}
			}
			/*
			Liberacion de memoria y de pipes

			*/

			
			unlink(nombresDePipesR[reducerAlQueEscribira]);
			for(int j = 0; j < cantReducers; j++)
				free(nombresDePipesR[j]);
			free(nombresDePipesR);

			for(int j = 0; j < numLineas; j++)
				free(matrizDeLogs[j]);

			free(matrizDeLogs);
			close(pipeMappers[0]);
			close(fd);
			//printf("\e[1;1H\e[2J");
			exit(0);	
				 
		}
		
	}
//----------------------------------------------------------------------------------------------------------------------------------------










//----------------------------------------------------While para que el usuario realice cuantas consultas desee---------------------------

	close(pipeMappers[0]);
	close(pipeReducers[1]);
	while(1){
		
		
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

			for(int i = 0; i < cantMappers; i++){

				copiadorDeStrings(consult.comando, &paramsM[i].cons.comando, 3);
				paramsM[i].cons.columna = consult.columna;
				paramsM[i].cons.valor = consult.valor;
				paramsM[i].desde = i * filasXProceso[0];
				paramsM[i].hasta = (filasXProceso[0] * i) + filasXProceso[i];
				aux = paramsM[i].hasta - paramsM[i].desde;
				write(pipeMappers[1], &paramsM[i], sizeof(struct ParametrosM));
				kill(childpidMappers[i], SIGUSR1);
				aux = 0;
				
			}

			for(int i = 0; i < cantReducers; i++){

				cuantos = read (pipeReducers[0], &aux, sizeof(int));
			  	if (cuantos == -1) {
			     		perror("proceso mapper:");
			     		exit(1);
			  	}
				salida += aux;
				aux = 0;
			}
			if(bitIntermedios == 0)
				printf("\e[1;1H\e[2J"); 
			gettimeofday(&end, NULL);

			/*
			Impresion de resultados finales

			*/
			printf("Salida: %d concurrencias en un tiempo de %d microsegundos usando procesos\n", salida,((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec)));
			salida = 0;
			printf("\n");		
			printf("\n");
	
		}else if( strcmp(mensajeUsuario, "2") == 0 ){	
				
			/*
			Liberacion de memoria

			*/

			close(pipeMappers[0]);
			close(pipeReducers[1]);
			for(int i = 0; i < numLineas; i++)
				free(matrizDeLogs[i]);
			
			free(matrizDeLogs);

			for(int i = 0; i < cantMappers; i++)
				kill(childpidMappers[i], SIGUSR2);
			

			for(int i = 0; i < cantReducers; i++)
				kill(childpidReducers[i], SIGUSR2);

			printf("\e[1;1H\e[2J"); 
			printf("Gracias usar el programa!\n");

			break;
		}else{
			printf("\e[1;1H\e[2J"); 
			printf("Comando no reconocido\n");
		}
		
	}
   	return 1;
}
//------------------------------------------------------------------------------------------------------------------------------
//-----------------------------------------------------------FINAL DE PROGRAMA--------------------------------------------------











//-------------------------------------------Implementacion de funciones--------------------------------------------------------

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

	if( (atoi(argv[2]) <=0) || (atoi(argv[3]) <=0) ||(atoi(argv[4]) > atoi(argv[3])) ){

		printf("Ingrese numeros validos para mappers y reducers\n");
		return bit;
	}

	if((atoi(argv[5]) < 0) || (atoi(argv[5]) > 1)){
		printf("Ingrese numeros validos para el bit intermedio\n");
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
      		perror("Error opening file");
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

int cuentaConcurrenciasM(struct ParametrosM *params, float **matrizDeLogs, struct Pares pares[]){

	fflush(NULL);
	int k = 0;
	if(strcmp(params->cons.comando, ">") ==0){
		for( int j = params->desde; j < params->hasta; j++){
				
			if(matrizDeLogs[j][params->cons.columna-1] > params->cons.valor){

				pares[k].clave = matrizDeLogs[j][0];
				pares[k].valor = matrizDeLogs[j][params->cons.columna-1];
				k = k + 1;
			}
							
		}
	}else if(strcmp(params->cons.comando, ">=") == 0 || strcmp(params->cons.comando, "=>") == 0){
		for( int j = params->desde; j < params->hasta; j++){
							
			if(matrizDeLogs[j][params->cons.columna-1] >= params->cons.valor){
				pares[k].clave = matrizDeLogs[j][0];
				pares[k].valor = matrizDeLogs[j][params->cons.columna-1];
				k = k + 1;
			}
							
		}
	}else if(strcmp(params->cons.comando, "<") == 0){
		for( int j = params->desde; j < params->hasta; j++){
							
			if(matrizDeLogs[j][params->cons.columna-1] < params->cons.valor){
				pares[k].clave = matrizDeLogs[j][0];
				pares[k].valor = matrizDeLogs[j][params->cons.columna-1];
				k = k + 1;
			}
							
		}
	}else if(strcmp(params->cons.comando, "<=") == 0 || strcmp(params->cons.comando, "=<") == 0){
		for( int j = params->desde; j < params->hasta; j++){
							
			if(matrizDeLogs[j][params->cons.columna-1] <= params->cons.valor){
				pares[k].clave = matrizDeLogs[j][0];
				pares[k].valor = matrizDeLogs[j][params->cons.columna-1];
				k = k + 1;
			}
							
		}

	}else if(strcmp(params->cons.comando, "=") == 0){
		for( int j = params->desde; j < params->hasta; j++){
							
			if(matrizDeLogs[j][params->cons.columna - 1] == params->cons.valor){
				pares[k].clave = matrizDeLogs[j][0];
				pares[k].valor = matrizDeLogs[j][params->cons.columna-1];
				k = k + 1;
			}
							
		}

	}else if(strcmp(params->cons.comando, "!=") == 0){
		for( int j = params->desde; j < params->hasta; j++){
							
			if(matrizDeLogs[j][params->cons.columna-1] != params->cons.valor){
				pares[k].clave = matrizDeLogs[j][0];
				pares[k].valor = matrizDeLogs[j][params->cons.columna-1];
				k = k + 1;
			}
							
		}

	}
	return k;

}

/*
Funcion: copiadorDeStrings
Entradas: string origen y string destino
Salidas: nada mas alla de el string origen copiado al string destino
Funcionamiento: se copia letra a letra el string origen al destino

*/
void copiadorDeStrings(char origen[], char *destino, int tam){

	for(int i = 0; i < tam; i++)
		destino[i] = origen[i];
}


/*
Funcion: conversorArchivos
Entradas: Arreglo de nombres que se va a inicializar, el nombre que se la va a colocar y el identificador unico de este
Salidas: nada mas alla de los nombres inicializados
Funcionamiento: Se busca el nombredel archivo en cuestion y se inicializa con un valor unico relacionado al proceso que lo va a usar

*/
void conversorArchivos(char **archivosMappers ,int nombreArchivo){

	int nombreArchivo2 = nombreArchivo;
        int n = floor(log10(nombreArchivo + 1)) + 1 + 5;
        archivosMappers[nombreArchivo]  =  (char *) malloc((n)*sizeof(char));
        for (int i = 0; i < n; i++, nombreArchivo /= 10 ){
        	archivosMappers[nombreArchivo2][i] = (nombreArchivo % 10) + 48;
	}
	archivosMappers[nombreArchivo2][n - 1] ='\0';
	archivosMappers[nombreArchivo2][n - 2] = 'e';
	archivosMappers[nombreArchivo2][n - 3] = 'p';
	archivosMappers[nombreArchivo2][n - 4] = 'i';
	archivosMappers[nombreArchivo2][n - 5] = 'p';
	
}

//------------------------------------------------------------------------------------------------------------------------------------------

