/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Programacion;

import Clases.Configuracion;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

/**
 *
 * @author Victor Diaz
 */


// Fío que se encarga de comprobar se hai novas notificacións da base de datos
class Listener extends Thread {
    
    private Configuracion accesoBBDD;
    private Connection conn;
    private org.postgresql.PGConnection pgconn;

    public Listener(Configuracion _accesoBBDD) throws SQLException     {
        
        this.accesoBBDD = _accesoBBDD;
        
        abrirConexion();
        
        this.pgconn = conn.unwrap(PGConnection.class);
        Statement stmt = conn.createStatement();
        stmt.execute("LISTEN novoarquivo;");
        stmt.close();        
    }

    public void run()     {
        
        System.out.println(">> Iniciando fío para escoitar notificacións de novos arquivos na base de datos.");
        
        try         {
            
            // proceso que se executa indefinidamente
            while (true)             {
                
                System.out.println(">>> Buscando novas notificacións...");
                
                // Comprobamos notificacións
                PGNotification notificacions[] = pgconn.getNotifications();

                if (notificacions != null)
                {
                    for (int i=0; i < notificacions.length; i++)
                        
                        if (notificacions[i].getName().equals("novoarquivo")) {
                            // Por cada notificación de novo arquivo gravámolo no disco
                            descargarNovoArquivo(notificacions[i].getParameter());
                        }                        
                }

                Thread.sleep(2000); // Agardamos 2 segundos para volver a comprobar notificacións
            }
            
        }
        
        catch (SQLException sqle)
        {
            sqle.printStackTrace();
        }
        catch (InterruptedException ie)
        {
            ie.printStackTrace();
        }
        
        System.out.println(">> Rematado o fío de escoitar notificacións.");
    }
    
    // Método para gravar no disco o novo arquivo e o seu contido na carpeta correspondente
    private void descargarNovoArquivo(String codigo) {
    
        // Buscamos na base de datos o novo arquivo e o directorio ao que pertence
        String sql = "select a.oid, a.nome, a.oiddirectorio, a.contido, d.nome " +
                "from arquivos a left join directorios d on a.oiddirectorio=d.oid " +
                "where a.oid=?;";
        
        try {
            
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setLong(1, Long.parseLong(codigo));
            ResultSet rs = ps.executeQuery();
            
            // Comprobamos se o directorio existe no disco
            while (rs.next()) {
                
                File directorio = new File(Main.separadorRutas(accesoBBDD.getApp().getDirectory() + rs.getString(5).substring((1))));
                
                if (!directorio.exists()) {
                    // se non existe creámolo, así como os directorios pai, se fixera falla
                    directorio.mkdirs();
                }
                          
                // gardamos o arquivo
                File arquivo = new File(Main.separadorRutas(accesoBBDD.getApp().getDirectory() + rs.getString(5).substring(1) + File.separator + rs.getString(2)));
                    
                OutputStream ficheiro = new FileOutputStream(arquivo);

                byte[] bytes = rs.getBytes("contido");

                ficheiro.write(bytes);
                ficheiro.close();

            }
                    
        } catch (Exception erro) {
            System.out.println("Erro descargando novo arquivo da base de datos: " + erro.toString());
        }
        
        
    }
    
    // Método para abrir unha conexión á base de datos
    private void abrirConexion() {
    
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
                System.out.println("Conexión insatisfactoria coa base de datos.");
            }
            
        } catch (SQLException erro) {
            System.out.println("Erro tratando de acceder á base de datos: " + erro.toString());
        }
        
    }
}