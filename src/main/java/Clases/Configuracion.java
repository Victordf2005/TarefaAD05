/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Clases;

/**
 *
 * @author Victor Diaz
 */

// Clase que conterá os datos de conexión á base de datos
// así como o directorio raiz de traballo
public class Configuracion {
    
    private ConfiguracionBBDD dbConnection;
    private ConfiguracionApp app;
    
    public Configuracion(){}

    public Configuracion(ConfiguracionBBDD bbdd, ConfiguracionApp app) {
        this.dbConnection = bbdd;
        this.app = app;
    }

    public void setDbConnection(ConfiguracionBBDD bbdd) {
        this.dbConnection = bbdd;
    }

    public void setApp(ConfiguracionApp app) {
        this.app = app;
    }

    
    public ConfiguracionBBDD getDbConnection() {
        return dbConnection;
    }

    public ConfiguracionApp getApp() {
        return app;
    }
    
    
    
}
