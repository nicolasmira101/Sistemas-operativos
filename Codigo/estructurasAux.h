//----------------------------------------------------------------Estructuras auxiliares:-------------------------------------


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
	struct Consulta cons;	

};
