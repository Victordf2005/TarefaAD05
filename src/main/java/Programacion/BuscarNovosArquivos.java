/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Programacion;

import Clases.Configuracion;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Victor Diaz
 */

/* Fío que se encarga de buscar periodicamente nas carpetas do disco
*  se hai novos arquivos para gravar na base de datos
*/
public class BuscarNovosArquivos extends Thread {
    
    private static Configuracion accesoBBDD;
    private static Connection conn;
    
    public BuscarNovosArquivos (Configuracion _accesoBBDD) {
    
        this.accesoBBDD= _accesoBBDD;
        
        abrirConexion();        
        
    }
    
    public void run() {
        
        // Proceso que se executa indefinidamente.
        while (true) {
            
            System.out.println("-- Iniciando novo fío buscando novos arquivos.");

            // Executamos o método da clase Main que carga directorios e arquivos na base de datos
            Main.cargarDirectorios(Main.separadorRutas(accesoBBDD.getApp().getDirectory()), conn);

            System.out.println("-- Rematando fío buscando novos arquivos.");
            
            try {
                Thread.sleep(5000); // agardamos 5 segundos para iniciar unha nova comprobación
            } catch (InterruptedException ex) {
                Logger.getLogger(BuscarNovosArquivos.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
                
    }
    
    // Método para abrir unha conexión á base de datos
    private static void abrirConexion() {
    
        String postgresql = "jdbc:postgresql://"
                + accesoBBDD.getDbConnection().getAddress()
                + "/"
                + accesoBBDD.getDbConnection().getName();
        
        Properties propiedades = new Properties();
        propiedades.setProperty("user", accesoBBDD.getDbConnection().getUser());
        propiedades.setProperty("password",accesoBBDD.getDbConnection().getPassword());
        
        try {
            conn = DriverManager.getConnection(postgresql, propiedades);
            
            if (conn == null) {
                System.out.println("Conexión insatisfactoria coa base de datos dentro do fío de execución.");
            }
            
        } catch (SQLException erro) {
            System.out.println("Erro tratando de acceder á base de datos dentro do fío de execución: " + erro.toString());
        }
        
    }
    
}
