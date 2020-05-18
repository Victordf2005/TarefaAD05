/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Programacion;

import Clases.Configuracion;
import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Victor Diaz
 */
public class Main  {
    
    private static Configuracion accesoBBDD;
    private static Connection conn;
    
    public static void main(String[] args) throws Exception {
        
        cargarConfiguracion();
        
        if (accesoBBDD != null) {
            mapearDirectorio();
            esperarNovosArquivos();        
            crearFuncion_E_Trigger();
            esperarNotificacions();
            pecharConexion();
        }        
        
    }
    
     
    private static void mapearDirectorio() {
        
        abrirConexion();
        
        /**
         * Comprobamos se a BBDD ten a táboa directorios e, de existir, se ten datos.
         * Se non existe ou non ten contido entendemos que debemos crear de novo as táboas
        */
        
        if (baseDatosBaleira()) {
            crearTaboas();  // Creamos as táboas
        }
        
        // Primeiro comprobamos se todos os directorios e arquivos do disco esán na base de datos
        cargarDirectorios(separadorRutas(accesoBBDD.getApp().getDirectory()), conn);

        // Comprobamos agora se todos os directorios e arquivos da base de datos están na súa ruta no disco
        descargarDirectorios();
          
        
    }
    
    /** Entendemos que a base de datos está baleira se non ten
     * a táboa directorios ou esta non ten rexistros (nin tan siquera o directorio "." */
    
    private static boolean baseDatosBaleira() {
        
        
        //Se pasa por aquí, a conexión foi satisfactoria
        
        int rexistros = 0;
        
        try {
                                   
            Statement sta = conn.createStatement();
            
            // comprobamos se existe a táboa directorios
            ResultSet rs = sta.executeQuery("Select count(*) from information_schema.columns where table_name='directorios' group by table_name");
            
            while (rs.next()) {
                rexistros = rs.getInt(1);
            }
            
            if (rexistros > 0) {
                // Existe a táboa directorios, comprobamos se ten datos            
                rs = sta.executeQuery("select count(*) from directorios;");

                while (rs.next()) {
                    rexistros = rs.getInt(1);
                }
                
            }
            
        } catch (Exception erro) {
            System.out.println("Erro comprobando se a base de datos está baleira. " + erro.toString());
            rematarExecucion();
        }
        
        // Devolvemos true se non ten rexistros a táboa directorios
        return (rexistros == 0);
        
    }
    
    
    private static void crearTaboas() {
        
        try {
                        
            Statement sta = conn.createStatement();
            
            String sql = "CREATE TABLE IF NOT EXISTS directorios (\"nome\" text NOT NULL) WITH OIDS;";                    
            sta.execute(sql);
                        
            sql = "CREATE TABLE IF NOT EXISTS arquivos (\"nome\" text NOT NULL, "
                    + "\"oiddirectorio\" integer, "
                    + "\"contido\" bytea) WITH OIDS;";
            sta.execute(sql);
            
            sta.executeUpdate("delete from arquivos;");
            
            
        } catch(Exception erro){
            System.out.println("Erro tratando de crear as táboas: " + erro.toString());
        }
        
    }
    
    // Función que comproba todos os arquivos e subdirectorios recursivamente
    protected static void cargarDirectorios(String carpeta, Connection con2) {
        
        // gardar a ruta ao directorio actual
        gravarDirectorio(carpeta, con2);
        
        // Obtemos o contido dese directorio
        File dir = new File(carpeta);        
        String[] lista = dir.list();
                
        // recorremos a lista de arquivos e directorios obtida
        for (int i = 0; i<lista.length; i++) {
            
            File f = new File(carpeta + File.separator + lista[i]);
            
            String rutaCompleta = carpeta + File.separator + lista[i];
                        
            if (f.isDirectory()) {
                // gravamos o directorio atopado
                gravarDirectorio(rutaCompleta, con2);
                cargarDirectorios(rutaCompleta, con2);
            } else {
                // gravamos o arquivo atopado 
                gravarArquivo(rutaCompleta, con2);
            }
            
        }
        
    
    }
    
    // Método que grava na base de datos o nome do directorio actual
    private static void gravarDirectorio(String carpeta, Connection con2) {
        
        if (!existeDirectorio(carpeta, con2)) {
            
            String sql = "insert into directorios (nome) values(?)";

            try {

                PreparedStatement ps = con2.prepareStatement(sql);
                ps.setString(1, rutaRelativa(carpeta));
                ps.executeUpdate();

            } catch (Exception erro) {
                System.out.println("Erro gravando directorio " + carpeta + " na base de datos: " + erro.toString());
            }
        }
        
    }
    
    // Función que comproba se xa existe o directorio na base de datos
    private static boolean existeDirectorio(String carpeta, Connection con2) {

        int rexistros = 0;

        String sql ="select count(*) from directorios where nome=?";
        
        try {

            PreparedStatement ps = con2.prepareStatement(sql);
            ps.setString(1, rutaRelativa(carpeta));
            ResultSet rs = ps.executeQuery();
            
            while (rs.next()) {
                rexistros = rs.getInt(1);
            }

        } catch (Exception erro) {
            System.out.println("Erro comprobando se xa existe o directorio na base de datos: " + erro.toString());
            rematarExecucion();
        }

        // Devolvemos true se hai un rexistro con ese nome na base de datos.
        return (rexistros > 0);
    }
    
    // Método que grava o nome e o contido dun arquivo, así como o código oid do directorio no que se atopa
    private static void gravarArquivo(String dirArquivo, Connection con2) {
        
        if (!existeArquivo(dirArquivo, con2)) {
            
            String sql = "insert into arquivos (nome, oiddirectorio, contido) values(?, ?, ?)";
            
            // Separamos en dúas variables a ruta completa e o nome do arquivo
            int ultimaBarra = dirArquivo.lastIndexOf(File.separator);
            String directorio = rutaRelativa(dirArquivo.substring(0, ultimaBarra));
            String arquivo = dirArquivo.substring(ultimaBarra + 1);

            // Recuperamos oid do directorio ao que pertence
            int oid = obterOidDirectorio(directorio, con2);

            try {

                File f = new File(dirArquivo);
                FileInputStream contido = new FileInputStream(f);

                PreparedStatement ps = con2.prepareStatement(sql);
                ps.setString(1, arquivo);   // nome do arquivo
                ps.setInt(2, oid);          // oid do directorio ao que pertence
                ps.setBinaryStream(3, contido, (int) f.length());   // contido do arquivo

                ps.executeUpdate();

            } catch (Exception erro) {
                System.out.println("Erro gravando arquivo " + arquivo + " na base de datos: " + erro.toString());
            }
            
        }
        
    }
    
    // Función que comproba se un arquivo xa existe no mesmo directorio
    protected static boolean existeArquivo(String dirArquivo, Connection con2) {
        
        int rexistros = 0;
        
        // Separamos en dúas variables a ruta completa e o nome do arquivo
        int ultimaBarra = dirArquivo.lastIndexOf(File.separator);
        String directorio = rutaRelativa(dirArquivo.substring(0, ultimaBarra));
        String arquivo = dirArquivo.substring(ultimaBarra + 1);
        
        // Recuperamos oid do directorio
        int oid = obterOidDirectorio(directorio, con2);
        
        try {
            
            String sql = "select count(*) from arquivos where nome=? and oiddirectorio=?;";

            PreparedStatement ps = con2.prepareStatement(sql);
            ps.setString(1, arquivo);
            ps.setLong(2, oid);
            ResultSet rst = ps.executeQuery();

            while (rst.next()) {
                rexistros = rst.getInt(1);
            }

        } catch (Exception erro) {
            System.out.println("Erro comprobando se xa existe o arquivo " + dirArquivo + " na base de datos: " + erro.toString());
            rematarExecucion();
        }

        return (rexistros > 0);
            
    }
    
    // Método que descarga os arquivos da base de datos que non existan no disco no mesmo directorio
    private static void descargarDirectorios() {
        
        String sql = "";
        
        try {
            
            // Creamos, se non existe o directorio raíz
            File raiz = new File(separadorRutas(accesoBBDD.getApp().getDirectory()));
            
            if (!raiz.exists()) {
                raiz.mkdirs();
            }
            
            // Primeiro comprobamos só os directorios
            Statement sta = conn.createStatement();
            sql = "select * from directorios where nome<>'.' order by nome;";
            ResultSet rs = sta.executeQuery(sql);
            
            while (rs.next()) {
                
                File f = new File(separadorRutas(accesoBBDD.getApp().getDirectory() + rs.getString(1).substring((1))));
                
                if (!f.exists()) {
                    f.mkdirs();
                }
            }
            
            // Comprobamos agora os arquivos
            // collemos todos os datos agás o contido do arquivo, xa que se pesa moito ralentiza a consulta
            sql = "select a.oid, a.nome, a.oiddirectorio, d.nome from arquivos a left join directorios d on a.oiddirectorio=d.oid;";
            rs = sta.executeQuery(sql);
            
            String sql2 = "";
            
            while (rs.next()) {
                
                File f = new File(separadorRutas(accesoBBDD.getApp().getDirectory() + rs.getString(4).substring(1) + File.separator + rs.getString(2)));
                
                if (!f.exists()) {
                    
                    // se o arquivo non existe buscamos o seu contido
                    sql2= "select contido from arquivos where oid=?;";
                    PreparedStatement ps2 = conn.prepareStatement(sql2);
                    ps2.setLong(1, rs.getLong(1));
                    ResultSet rs2 = ps2.executeQuery();
                    
                    // gravamos o no arquivo no disco
                    while (rs2.next()) {
                        
                        OutputStream ficheiro = new FileOutputStream(f);

                        byte[] bytes = rs2.getBytes("contido");

                        ficheiro.write(bytes);
                        ficheiro.close();
                    }
                }
            }
        
        } catch (Exception erro) {
            System.out.println("Erro descargando arquivo da base de datos: " + erro.toString());
        }
        
    }
    
    /* Método que crea un fío que se encarga de comprobar
    *  se hai novos arquivos no disco
    */
    private static void esperarNovosArquivos() {
        
        System.out.println("Buscando novos arquivos.");
        
        BuscarNovosArquivos buscador = new BuscarNovosArquivos(accesoBBDD);
        buscador.start();
        
    }
    
    // Método que crea un fío no que se agarda por notificacións da base de datos
    private static void esperarNotificacions() {
                
        System.out.println("Esperando notificacións.");
        
        try {
            Listener escoita = new Listener(accesoBBDD);
            escoita.start();
        } catch (SQLException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            rematarExecucion();
        }
        
    }
    
    // Función que devolve o oid dun directorio dado
    protected static int obterOidDirectorio(String directorio, Connection con2) {
    
        int retorno = 0;
        
        try {
                                   
            Statement sta = con2.createStatement();
            
            ResultSet rs = sta.executeQuery("Select oid from directorios where nome='" + directorio +"'");
            
            while (rs.next()) {
                retorno = rs.getInt(1);
            }
        
        } catch (Exception erro) {
            System.out.println("Erro buscando OID do directorio '" + directorio + "': " + erro.toString());
            rematarExecucion();
        }
        
        return retorno;
        
    }
    
    
    // Devolve a ruta relativa do directorio quitando a ruta
    // ao directorio principal e substituíndo esta por un "."
    public static String rutaRelativa(String directorio) {
    
        return directorio.replaceFirst(separadorRutas(accesoBBDD.getApp().getDirectory()), ".");
        
    }
    
    // Método que abre unha conexión coa base de datos
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
                System.out.println("Conexión insatisfactoria coa base de datos.");
                rematarExecucion();
            }
            
        } catch (SQLException erro) {
            System.out.println("Erro tratando de acceder á base de datos: " + erro.toString());
            rematarExecucion();            
        }
        
    }
    
    // Método para pechar a conexión á base de datos
    private static void pecharConexion() {
          
        try {
            conn.close();
        } catch (Exception erro) {
            System.out.println("Erro pechando a conexión: " + erro.toString());
        }
        
    }
    
    /* Método que carga a configuración de acceso á base de datos
    *  e directorio de traballo desde un arquivo json
    */
    private static void cargarConfiguracion(){
    
        // Cargamos os datos de configuración de acceso á BBDD
        File arquivoConf = new File("config.json");
        
        // Comprobamos se existe o arquivo
        if (arquivoConf.exists()) {
            
            try {
                // Creamos fluxo de datos
                FileReader fluxoDatos = new FileReader(arquivoConf);
                BufferedReader entrada = new BufferedReader(fluxoDatos);
                
                StringBuilder jsonBuilder = new StringBuilder();
                String linea;
                
                // Lemos o arquivo liña a liña
                while ((linea = entrada.readLine()) != null) {
                    jsonBuilder.append(linea).append("\n");
                }
                
                // Pechamos o arquivo
                entrada.close();
                
                // Construimos a cadea json
                String json = jsonBuilder.toString();
                Gson gson = new Gson();
                
                // Creamos o obxecto de configuración de acceso á base de datos
                // cos datos lidos.
                accesoBBDD = gson.fromJson(json, Configuracion.class);
                
            }
            catch (IOException erro) {
                System.out.println("Erro cargando a configuración de conexión á base de datos.");
            }
        }
        
    }
    
    // Rematamos a execución
    private static void rematarExecucion() {

        try  {
            conn.close();
        } catch (Exception erro) {
         //Estamos rematando. Non importa o erro intentando pechar a conexión
        }
        
        System.out.println("*** FIN DE EXECUCION ***");
        System.exit(0);
        
    }
    
    /* Método que crea unha función e un trigger para lanzar unha notificación
    *  cada vez que se engada un arquivo á base de datos
    */
    private static void crearFuncion_E_Trigger() {
    
        System.out.println("Creando funcion e trigger.");
        
        String funcion =
                "create or replace function fn_novo_arquivo() " +
                "returns trigger as $$ " +
                "begin " +
                    "perform pg_notify('novoarquivo', new.oid::text); " +
                "return new; " +
                "end; "+
                "$$ language plpgsql;";
                
        try {
            
            CallableStatement crearFuncion = conn.prepareCall(funcion);
            crearFuncion.execute();
            crearFuncion.close();
            
            String trigger = "drop trigger if exists novo_arquivo_trig on arquivos; "+
                    "create trigger novo_arquivo_trig " +
                    "after insert on arquivos " +
                    "for each row " +
                        "execute procedure fn_novo_arquivo(); ";
            
            CallableStatement crearTrigger = conn.prepareCall(trigger);
            crearTrigger.execute();
            crearTrigger.close();
                                    
        } catch (SQLException erro) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, erro);
            rematarExecucion();
        }
        
    }
    
    // Función para substituir o separador de rutas de acordo co SO anfitrión
    protected static String separadorRutas(String ruta) {
        
        String separador = "\\";
        
        try {
            if (File.separator.equals(separador)) {
                separador = "/";
            }

            return ruta.replaceAll(separador, File.separator);
        
        } catch (Exception erro) {
            return ruta.replaceAll(separador+separador, File.separator);
        }
            
    }
        
}

