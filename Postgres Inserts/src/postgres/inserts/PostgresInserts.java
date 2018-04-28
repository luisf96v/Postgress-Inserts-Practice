package postgres.inserts;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgresInserts {

    private Connection con;

    public PostgresInserts() throws ClassNotFoundException, SQLException {

        Class.forName("org.postgresql.Driver");

        String database = "tarea";
        String usuario = "postgres";
        String contra = "root";
        try {

            con = DriverManager.
                    getConnection(
                            "jdbc:postgresql://localhost:5432/" + database,
                            usuario,
                            contra
                    );
            System.out.println("Conectado Postgre Server");
        } catch (SQLException ex) {
            System.out.println("Error Conectando Postgre Server");
            System.err.println(ex);
            throw ex;
        }
        System.out.println("Conectado en Base de Datos: " + database);

        Statement stm = con.createStatement();
        stm.execute("drop schema if exists inserts cascade;");
        stm.execute("create schema if not exists inserts;");

        con.setSchema("inserts");
        System.out.println("Usando Schema: " + con.getSchema());

        System.out.println("---------------------------------------------");
        System.out.println("Creando Tablas");

        stm.execute(
                "create table if not exists vendedor("
                + "     codigo int,\n"
                + "	nombre varchar(30),\n"
                + "	constraint pkVendedor primary key (codigo));"
                + "create table if not exists cliente(\n"
                + "	ced int,\n"
                + "	nombre varchar(30),\n"
                + "	vendedor int,\n"
                + "	zona int,\n"
                + "	constraint pkCliente primary key (ced)\n"
                + ");" + "create table if not exists Facturas(\n"
                + "	numero int,\n"
                + "	fecha date,\n"
                + "	cliente int,\n"
                + "	constraint pkFactura primary key (numero)\n"
                + ");" + "create table if not exists Productos (\n"
                + "	codigo int,\n"
                + "	nombre varchar(30),\n"
                + "	precio int,\n"
                + "	constraint pkProducto primary key (codigo)\n"
                + ");"
                + "create table if not exists Productos (\n"
                + "	codigo int,\n"
                + "	nombre varchar(30),\n"
                + "	precio int,\n"
                + "	constraint pkProducto primary key (codigo)\n"
                + ");"
                + "create table if not exists Zona(\n"
                + "	codigo int,\n"
                + "	nombre varchar(30),\n"
                + "	constraint pkZona primary key (codigo)\n"
                + ");"
                + "create table if not exists Detalle(\n"
                + "	cantidad int,\n"
                + "	producto int,\n"
                + "	factura int,\n"
                + "	constraint pkDetalle primary key (producto, factura)\n"
                + ");"
                + "ALTER TABLE Cliente ADD CONSTRAINT fkCliente\n"
                + "    FOREIGN KEY (vendedor)\n"
                + "    REFERENCES Vendedor (codigo)  \n"
                + "    NOT DEFERRABLE \n"
                + "    INITIALLY IMMEDIATE\n"
                + ";\n"
                + "\n"
                + "ALTER TABLE Cliente ADD CONSTRAINT fkZona\n"
                + "    FOREIGN KEY (zona)\n"
                + "    REFERENCES Zona (codigo)  \n"
                + "    NOT DEFERRABLE \n"
                + "    INITIALLY IMMEDIATE\n"
                + ";\n"
                + "\n"
                + "ALTER TABLE Facturas ADD CONSTRAINT fkFactura\n"
                + "    FOREIGN KEY (cliente)\n"
                + "    REFERENCES Cliente (ced)  \n"
                + "    NOT DEFERRABLE \n"
                + "    INITIALLY IMMEDIATE\n"
                + ";\n"
                + "\n"
                + "ALTER TABLE Detalle ADD CONSTRAINT fkDetalle1\n"
                + "    FOREIGN KEY (factura)\n"
                + "    REFERENCES Facturas (numero)  \n"
                + "    NOT DEFERRABLE \n"
                + "    INITIALLY IMMEDIATE\n"
                + ";\n"
                + "\n"
                + "ALTER TABLE Detalle ADD CONSTRAINT fkDetalle2\n"
                + "    FOREIGN KEY (producto)\n"
                + "    REFERENCES Productos (codigo)  \n"
                + "    NOT DEFERRABLE \n"
                + "    INITIALLY IMMEDIATE\n"
                + ";");

        System.out.println("Tablas creadas exitosamente.");
        System.out.println("---------------------------------------------");
        System.out.println("Generando inserts.");

        // VENDEDORES
        try {
            stm.execute("INSERT INTO Vendedor (codigo, nombre) VALUES\n"
                    + "(1, 'Ana'),(2, 'Pedro'),(3, 'Juan'),(4, 'Maria'),\n"
                    + "(5, 'Josue'),(6, 'Jose'),(7, 'Pablo'),(8, 'Daniela'),\n"
                    + "(9, 'Anthony'),(10, 'Jason');");
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        //ZONAS
        try {
            stm.execute("  \n"
                    + "INSERT INTO Zona (codigo, nombre) VALUES\n"
                    + "  (1, 'norte'),(2, 'sur'),(3, 'central'),(4, 'oeste');");
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        //CLIENTES
        try {
            int i = 1111;
            stm.execute("INSERT INTO Cliente (ced, nombre, vendedor, zona)\n"
                    + "VALUES\n"
                    + "  (" + cast(i++) + ", 'Antonio', 1 , 1),(" + cast(i++) + ", 'Manuel'  , 1 , 1),(" + cast(i++) + ", 'Francisco', 1 , 1),(" + cast(i++) + ", 'David' , 1 , 1),(" + cast(i++) + ", 'Javier', 2 , 1),\n"
                    + "  (" + cast(i++) + ", 'Carlos' , 2 , 1),(" + cast(i++) + ", 'Daniel'  , 2 , 1),(" + cast(i++) + ", 'Miguel'   , 2 , 1),(" + cast(i++) + ", 'Rafael', 3 , 1),(" + cast(i++) + ", 'Alejandro', 3 , 1),\n"
                    + "  (" + cast(i++) + ", 'Angel'  , 3 , 2),(" + cast(i++) + ", 'Fernando', 3 , 2),(" + cast(i++) + ", 'Luis'     , 4 , 2),(" + cast(i++) + ", 'Sergio', 4 , 2),(" + cast(i++) + ", 'Jorge', 4 , 3),\n"
                    + "  (" + cast(i++) + ", 'Alberto', 4 , 3),(" + cast(i++) + ", 'Diego'   , 5 , 3),(" + cast(i++) + ", 'Carmen'   , 5 , 3),(" + cast(i++) + ", 'Josefa', 5 , 2),(" + cast(i++) + ", 'Isabel', 5 , 3),\n"
                    + "  (" + cast(i++) + ", 'Teresa' , 6 , 3),(" + cast(i++) + ", 'Dolores' , 6 , 3),(" + cast(i++) + ", 'Pilar'    , 6 , 3),(" + cast(i++) + ", 'Laura' , 6 , 3),(" + cast(i++) + ", 'Francisca', 7 , 3),\n"
                    + "  (" + cast(i++) + ", 'Antonia', 7 , 3),(" + cast(i++) + ", 'Cristina', 7 , 3),(" + cast(i++) + ", 'Lucia'    , 7 , 3),(" + cast(i++) + ", 'Luisa' , 8 , 3),(" + cast(i++) + ", 'Elena', 8 , 3),\n"
                    + "  (" + cast(i++) + ", 'Mercede', 8 , 4),(" + cast(i++) + ", 'Sara'    , 8 , 3),(" + cast(i++) + ", 'Rosa'     , 9 , 4),(" + cast(i++) + ", 'Raquel', 9 , 4),(" + cast(i++) + ", 'Paula', 9 , 4),\n"
                    + "  (" + cast(i++) + ", 'Emilia' , 9 , 4),(" + cast(i++) + ", 'Gloria'  , 10 , 4),(" + cast(i++) + ", 'Mario'   , 10, 4),(" + cast(i++) + ", 'Julieta',10 ,4),(" + cast(i++) + ", 'Jasmin', 10 , 4);");
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        //PRODUCTOS
        try {
            stm.execute("INSERT INTO Productos (codigo, nombre, precio) VALUES\n"
                    + "  (1, 'Camiseta', 10000),(2, 'Pantalon', 20000),(3, 'Perfume', 30000),(4, 'Gorra', 35000),(5, 'Zapatos', 60000),\n"
                    + "  (6, 'Billetera', 20000),(7, 'Aspirina', 15000),(8, 'CardioAspirina', 5000),(9, 'Sueter', 35000),(10, 'Bolso',30000),\n"
                    + "  (11, 'Lentes', 50000),(12, 'Corbata', 25000),(13, 'Traje', 70000),(14, 'Portafolios', 50000),(15, 'Reloj', 80000),\n"
                    + "  (16, 'Blusa', 10000),(17, 'Vestido', 35000),(18, 'Collar', 40000),(19, 'Pulsera', 25000),(20, 'Sombrilla', 15000); \n"
                    + "  ");
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }

        int id_factura = 0;

        for (int id_cliente = 1111; id_cliente <= 1150; id_cliente++) {

            for (int mes = 1; mes <= 12; mes++) {
                int dia1 = (int) ((Math.random() * ((28 - 1) + 1)) + 1);
                int dia2 = (int) ((Math.random() * ((28 - 1) + 1)) + 1);
                int fac1 = id_factura++;
                int fac2 = id_factura++;

                stm.execute(
                        "INSERT INTO Facturas (numero, fecha , cliente) VALUES"
                        + "(" + cast(fac1) + ",'2017-" + cast(mes) + "-" + cast(dia1) + "', " + cast(id_cliente) + "),"
                        + "(" + cast(fac2) + ",'2017-" + cast(mes) + "-" + cast(dia2) + "', " + cast(id_cliente) + ");"
                );

                
                int cant1 = (int) ((Math.random() * ((20 - 4) + 1)) + 4);
                int cant2 = (int) ((Math.random() * ((20 - 4) + 1)) + 4);
                for (int i = 0; i < cant1 ; i++) {
                    try {
                        stm.execute(
                                "INSERT INTO Detalle (cantidad, producto , factura) VALUES"
                                + "(" + cast((int) ((Math.random() * ((20 - 1) + 1)) + 1))
                                + "," + cast((int) ((Math.random() * ((20 - 3) + 1)) + 3))
                                + "," + cast(fac1) + ")"
                        );
                    } catch (SQLException ex) {
                    }
                }

                for (int i = 0; i < cant2; i++) {
                    try {
                        stm.execute(
                                "INSERT INTO Detalle (cantidad, producto , factura) VALUES"
                                + "(" + cast((int) ((Math.random() * ((20 - 1) + 1)) + 1))
                                + "," + cast((int) ((Math.random() * ((20 - 3) + 1)) + 3))
                                + "," + cast(fac2) + ")"
                        );
                    } catch (SQLException ex) {
                    }
                }
            }

        }

        System.out.println(
                "Inserts ingresados exitosamente.");
        System.out.println(
                "---------------------------------------------\n");

        stm.close();

        con.close();

    }

    private String cast(int i) {
        return String.valueOf(i);
    }

    public static void main(String[] args) {
        try {
            PostgresInserts postgresInserts = new PostgresInserts();
        } catch (ClassNotFoundException | SQLException e) {
            System.err.println(e);
        }
    }

}
