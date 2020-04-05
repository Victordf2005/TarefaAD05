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

// Clase que contera a ruta ao directorio raiz de traballo
public class ConfiguracionApp {
    
    private String directory;
    
    public ConfiguracionApp(){};
    
    public ConfiguracionApp(String parametro) {this.directory = parametro;}
    
    public void setDirectory(String parametro) {this.directory = parametro;}
    
    public String getDirectory() {return this.directory;}
        
}
